# Convert a file from the pathfinder2 format to the CoFi csv format
"""
In short the pathfinder2 format is:
[from] [to] [token_owner] [capacity].
The "token_owner" is one of [from] or [to] basically indicates whether it's an amount to repay or a capacity to increase.

And we want to route a max-flow payment from [sender] [receiver] passed as a parameter.
If we have two from/to with positive amount, we should probably add them up.

The resulting CoFi CSV format should be:
[debtor] [creditor] [amount]

What's the mapping of from/to in the context of payment, and debtor/creditor? 

We want to transform the payment capacity problem into an equivalent setoffs problem.
Basically we model the capacity as invoices to each other that would net out if no further payments are made. Instead of a capacity to create debt, we have the capacity to repay an existing debt.

The borrower (debtor) is thus the "from" and the debtor is the "to" for each edge.

Finally, in order to model the payment, we have to do something non-obvious. We should add an edge in the direction "t" to "receiver". The reason why is that MTCS is looking for cycles, and if there is any capacity from 

what we do is add an edge in the opposite direction. 
edge from  we can add an infinite edge, from to
"""

import json
import csv
from collections import defaultdict
import networkx as nx
import struct

def read_pathfinder2_edges_csv(pathname):
    tbl = defaultdict(lambda: defaultdict(lambda:0))
    cnt = 0
    with open(pathname, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for (frm,to,tok,cap) in reader:
            cap = int(cap,16)
            tbl[frm][to] += cap
            cnt += 1
        print(f'Read {cnt} edges from {pathname}')

    # Read from csv
    return tbl

def graph_copy(tbl):
    return dict((frm, dict((to, amt) for to, amt in col.items())) for frm, col in tbl.items())

def write_cofi_edges_csv(tbl, pathname):

    all_nodes = set()
    for frm, col in tbl.items():
        all_nodes.add(frm)
        all_nodes.update(col.keys())
    index = dict((addr,idx) for idx, addr in enumerate(all_nodes))
    
    cnt = 0
    with open(pathname, 'w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['debtor','creditor','amount'])
        for frm, col in tbl.items():
            for to, amt in col.items():
                if frm == to: continue
                amt = int(amt//1e8)
                if amt <= 0: continue
                writer.writerow([index[frm], index[to], amt])
                cnt += 1
    print(f'wrote {cnt} edges to {pathname}')

#if not 'g' in globals():
#    g = read_pathfinder2_edges_csv('./graph_at_20230523_15_00.csv')


"""
uint32: number_of_addresses
[ bytes20: address ] * number_of_addresses
uint32: number_of_organizations
[ uint32: address_index ] * number_of_organizations
uint32: number_of_trust_edges
[
uint32: user_address_index
uint32: send_to_address_index
uint8: trust_limit_percentage
] * number_of_trust_edges
uint32: number_of_balances
[
uint32: user_address_index
uint32: token_owner_address_index
uint256: balance
] * number_of_balances
"""
def read_pathfinder2_safes_bin(pathname):
    tbl_trust    = defaultdict(lambda: defaultdict(lambda:0))
    tbl_utilized = defaultdict(lambda: defaultdict(lambda:0))
    
    cnt = 0
    with open(pathname, 'rb') as buf:
        number_of_addresses, = struct.unpack('>I', buf.read(4))

        addr = []
        for i in range(number_of_addresses):
            address, = struct.unpack('>20s', buf.read(20))
            addr.append(hex(int.from_bytes(address, byteorder='big')))

        organizations = set()
        number_of_organizations, = struct.unpack('>I', buf.read(4))
        for i in range(number_of_organizations):
            organization, = struct.unpack('>I', buf.read(4))
            organizations.add(organization)

        number_of_trust_edges, = struct.unpack('>I', buf.read(4))
        for i in range(number_of_trust_edges):
            frm, = struct.unpack('>I', buf.read(4))
            to , = struct.unpack('>I', buf.read(4))
            trust_limit_percentage, = struct.unpack('>B', buf.read(1))
            tbl_trust[addr[frm]][addr[to]] = trust_limit_percentage

        number_of_balances, = struct.unpack('>I', buf.read(4))
        for i in range(number_of_balances):
            to , = struct.unpack('>I', buf.read(4))
            frm, = struct.unpack('>I', buf.read(4))
            # These numbers are encoded as <number of nonzero bytes as uint8><nonzero bytes>,
            # i.e. 1 is encoded as 0x0101 while 256 is encoded as 0x020100.
            bal_size, = struct.unpack('>B', buf.read(1))
            balance, = struct.unpack(f'>{bal_size}s', buf.read(bal_size))
            tbl_utilized[addr[frm]][addr[to]] = int.from_bytes(balance, byteorder='little')

    print(f'Read {number_of_trust_edges} edges from {pathname}')

    # Read from csv
    return tbl_trust, tbl_utilized, organizations

if not 'tbl_trust' in globals():
    tbl_trust, tbl_uti, organizations = read_pathfinder2_safes_bin('./graph_at_20230523_15_00.db')

# returns how much of their own tokens a user can send to receiver.
# Adapted from compute_edges
#   https://github.com/CirclesUBI/pathfinder2/blob/dev/src/safe_db/db.rs#L32
# and trust_transfer_limit
#   https://github.com/CirclesUBI/pathfinder2/blob/dev/src/types/safe.rs#L21

def trust_transfer_limit(frm, to, trust_percentage):
    if frm == to or to in organizations:
        return tbl_uti[frm][frm]

    # How much of our tokens do they already have?
    receiver_balance = tbl_uti[to][frm]

    # QUESTION: what's the point of scaling it this way?
    scaled_receiver_balance = receiver_balance * (100 - trust_percentage) // 100

    # How many of their own tokens do they have? Scale this by the percentage they'll trust us.
    # This should be a maximum
    amount = tbl_uti[to][to] * trust_percentage // 100

    # We already exceed the maximum!!
    if amount < receiver_balance: return 0
    
    # Can't send more of our tokens than we have.
    # Why are we substracting the scaled amount?
    return min(amount - scaled_receiver_balance, tbl_uti[frm][frm])

# Build a NetworkX graph
# Compute the trust

graph = nx.DiGraph()
all_addrs = set(tbl_trust.keys()); all_addrs.update(tbl_uti.keys())
tbl_cap    = defaultdict(lambda: defaultdict(lambda:0))
for frm in all_addrs:
    uti = tbl_uti[frm]
    trusts = tbl_trust[frm]
    all_ = set(uti.keys()); all_.update(trusts.keys())
    for to in all_:
        if frm == to: continue
        utilized = uti[to]
        trust_percentage = trusts[to]
        capacity = trust_transfer_limit(frm, to, trust_percentage)
        tbl_trust[frm][to] = trust_percentage
        if utilized > 0:
            graph.add_edge(frm,to, weight=utilized, capacity=capacity)


print('writing')
write_cofi_edges_csv(tbl_uti, 'graph_at_20230523_15_00.cofi.csv')

bydeg = sorted(dict(graph.degree()).items(), key=lambda f:f[1],reverse=True)
        
# Try flow
#maximum_flow = nx.flow.maximum_flow(graph,
#                                    '0x31876ca5d1c64d671d2f70b5c26aba6afa48907c',
#                                    '0x580816b8beb4d1bca5b48af07cd989b6ffed6904')
#maximum_flow = nx.flow.maximum_flow(graph,
#                                    '0x9BA1Bcd88E99d6E1E03252A70A63FEa83Bf1208c'.lower(),
#                                    '0x42cEDde51198D1773590311E2A340DC06B24cB37'.lower())

# Compute 
#print(f"Maximum flow: {maximum_flow[0]}")
print(f'nodes: {len(graph.nodes())} edges: {len(graph.edges())}')
