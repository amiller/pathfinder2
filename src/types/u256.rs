use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::{Add, AddAssign, Neg, Sub, SubAssign};

use num_bigint::BigUint;

#[derive(Clone, Copy, Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct U256([u128; 2]);

impl U256 {
    pub const fn new(high: u128, low: u128) -> U256 {
        U256([high, low])
    }
    pub const MAX: U256 = U256::new(u128::MAX, u128::MAX);
    #[allow(dead_code)]
    pub fn to_decimal(self) -> String {
        let value = BigUint::from(self.0[0]) << 128 | BigUint::from(self.0[1]);
        format!("{}", value)
    }
}

impl From<u128> for U256 {
    fn from(item: u128) -> Self {
        U256([0, item])
    }
}

// TODO str is using unicode stuff - maybe we should use Vec<u8> for efficiency reasons?
impl From<&str> for U256 {
    fn from(item: &str) -> Self {
        if item.starts_with("0x") {
            let len = item.len();
            assert!(len <= 2 + 32 + 32, "{}", len);
            let low_start = if len >= 2 + 32 { len - 32 } else { 2 };
            let low_hex = &item[low_start..];
            // disallow + and - prefixes
            assert!(
                low_hex.as_bytes().get(0) != Some(&54) && low_hex.as_bytes().get(0) != Some(&43)
            );
            let low = if low_hex.is_empty() {
                0
            } else {
                u128::from_str_radix(low_hex, 16).unwrap()
            };
            let high_start = if len >= 2 + 32 + 32 { len - 64 } else { 2 };
            let high_hex = &item[high_start..low_start];
            // disallow + and - prefixes
            assert!(
                high_hex.as_bytes().get(0) != Some(&54) && high_hex.as_bytes().get(0) != Some(&43)
            );
            let high = if high_hex.is_empty() {
                0
            } else {
                u128::from_str_radix(high_hex, 16).unwrap()
            };
            U256([high, low])
        } else {
            let digits = item.parse::<num_bigint::BigUint>().unwrap().to_u64_digits();
            assert!(digits.len() <= 4);
            U256([
                u128::from(*digits.get(3).unwrap_or(&0)) << 64
                    | u128::from(*digits.get(2).unwrap_or(&0)),
                u128::from(*digits.get(1).unwrap_or(&0)) << 64
                    | u128::from(*digits.get(0).unwrap_or(&0)),
            ])
        }
    }
}

impl Add for U256 {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        let (low, carry) = self.0[1].overflowing_add(rhs.0[1]);
        let mut high = self.0[0].wrapping_add(rhs.0[0]);
        if carry {
            high = high.wrapping_add(1);
        }
        Self([high, low])
    }
}

impl AddAssign for U256 {
    fn add_assign(&mut self, rhs: U256) {
        *self = *self + rhs;
    }
}

impl Neg for U256 {
    type Output = Self;
    fn neg(self) -> Self {
        let result = U256([!self.0[0], !self.0[1]]);
        result + U256::from(1)
    }
}

impl Sub for U256 {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self {
        self + (-rhs)
    }
}

impl SubAssign for U256 {
    fn sub_assign(&mut self, rhs: Self) {
        *self = *self - rhs;
    }
}

impl Display for U256 {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if self.0[0] == 0 {
            write!(f, "{:#x}", self.0[1])
        } else {
            write!(f, "{:#x}{:032x}", self.0[0], self.0[1])
        }
    }
}

#[cfg(test)]
mod test {
    use super::U256;
    #[test]
    fn to_string() {
        assert_eq!(format!("{}", U256::from(0)), "0x0");
        assert_eq!(
            format!("{}", U256::from(u128::MAX)),
            "0xffffffffffffffffffffffffffffffff"
        );
    }

    #[test]
    fn add() {
        assert_eq!(
            format!("{}", U256::from(u128::MAX) + U256::from(u128::MAX)),
            "0x1fffffffffffffffffffffffffffffffe"
        );
        let mut x = U256::from(u128::MAX);
        x += U256::from(1);
        assert_eq!(format!("{x}"), "0x100000000000000000000000000000000");
    }

    #[test]
    fn compare() {
        assert!(U256::from(0) < U256::from(1));
        assert!(U256::from("0x100000000000000000000000000000000") > U256::from(1));
    }

    #[test]
    fn from_hex() {
        assert_eq!(U256::from("0x"), U256::from(0));
        assert_eq!(U256::from("0x1"), U256::from(1));
        assert_eq!(U256::from("0x01"), U256::from(1));
        assert_eq!(
            U256::from("0x1fffffffffffffffffffffffffffffffe"),
            U256::from(u128::MAX) + U256::from(u128::MAX)
        );
        assert_eq!(
            U256::from("0x001fffffffffffffffffffffffffffffffe"),
            U256::from(u128::MAX) + U256::from(u128::MAX)
        );
        assert_eq!(
            U256::from("0x100000000000000000000000000000000"),
            U256::from(u128::MAX) + U256::from(1)
        );
    }

    #[test]
    fn from_decimal() {
        assert_eq!(U256::from("0"), U256::from(0));
        assert_eq!(U256::from("10"), U256::from(10));
        assert_eq!(
            U256::from("680564733841876926926749214863536422910"),
            U256::from(u128::MAX) + U256::from(u128::MAX)
        );
        assert_eq!(
            U256::from("000680564733841876926926749214863536422910"),
            U256::from(u128::MAX) + U256::from(u128::MAX)
        );
        assert_eq!(
            U256::from("340282366920938463463374607431768211456"),
            U256::from(u128::MAX) + U256::from(1)
        );
    }

    #[test]
    fn to_decimal() {
        assert_eq!(U256::from("0").to_decimal(), "0");
        assert_eq!(
            U256::from("680564733841876926926749214863536422910").to_decimal(),
            "680564733841876926926749214863536422910"
        );
        assert_eq!(
            U256::from("000680564733841876926926749214863536422910").to_decimal(),
            "680564733841876926926749214863536422910"
        );
        assert_eq!(
            U256::from("340282366920938463463374607431768211456").to_decimal(),
            "340282366920938463463374607431768211456"
        );
    }
}
