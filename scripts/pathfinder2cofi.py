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
                amt = int(amt//1e8)
                if amt <= 0: continue
                writer.writerow([index[frm], index[to], amt])
                cnt += 1
    print(f'wrote {cnt} edges to {pathname}')

g = read_pathfinder2_edges_csv('./graph_at_20230523_15_00.csv')
write_cofi_edges_csv(g, 'graph_at_20230523_15_00.cofi.csv')

