use std::fmt;

use eyre::eyre;
use ruint::aliases::U256;
use tokio_postgres::types::{FromSql, Type};

pub enum Int {
    Pos(U256),
    Neg(U256),
}

impl fmt::Display for Int {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Int::Neg(n) => write!(f, "-{}", n),
            Int::Pos(n) => write!(f, "{}", n),
        }
    }
}

impl<'a> FromSql<'a> for Int {
    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }

    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        match *ty {
            Type::NUMERIC => {
                if raw.len() < 8 {
                    return Err(eyre!("numeric header too small").into());
                }

                // Parse header
                let ndigits = u16::from_be_bytes(raw[0..2].try_into()?);
                let weight = i16::from_be_bytes(raw[2..4].try_into()?);
                let sign = u16::from_be_bytes(raw[4..6].try_into()?);
                let _scale = u16::from_be_bytes(raw[6..8].try_into()?);

                // Check for special values (NaN, +/-Infinity)
                const NUMERIC_SPECIAL: u16 = 0xC000;
                if (sign & NUMERIC_SPECIAL) == NUMERIC_SPECIAL {
                    return Err(eyre!("special numeric values not supported").into());
                }

                // Read all groups
                let mut value = U256::from(0u64);
                let mut raw_cursor = &raw[8..];
                for _ in 0..ndigits {
                    if raw_cursor.len() < 2 {
                        return Err(eyre!("unexpected end of numeric data").into());
                    }
                    let group = u16::from_be_bytes(raw_cursor[0..2].try_into()?);
                    raw_cursor = &raw_cursor[2..];

                    // Each group represents a 4-digit number in base 10000
                    value = value * U256::from(10000u64) + U256::from(group as u64);
                }

                // Apply weight (each weight unit represents 10000)
                if weight >= 0 {
                    value = value * U256::from(10000u64).pow(U256::from(weight as u64));
                }

                // Handle sign
                if sign == 0x4000 {
                    Ok(Int::Neg(value))
                } else {
                    Ok(Int::Pos(value))
                }
            }
            _ => Err(eyre!("unable to decode").into()),
        }
    }
}
