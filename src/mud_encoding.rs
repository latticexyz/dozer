use alloy::primitives::{FixedBytes, B256};
use eyre::{eyre, Result};

pub struct Data<'a> {
    encoded_lengths: FixedBytes<32>,
    dynamic_data: &'a [u8],
    static_data: &'a [u8],
}

fn dec(s: &[u8]) -> usize {
    s.iter().fold(0, |n, b| n << 8 | *b as usize)
}

impl<'a> Data<'a> {
    pub fn new(
        static_data: &'a [u8],
        encoded_lengths: FixedBytes<32>,
        dynamic_data: &'a [u8],
    ) -> Result<Self> {
        if dec(&encoded_lengths[25..]) != dynamic_data.len() {
            return Err(eyre!("encoded_lengths does not match dynamic_data"));
        }
        if encoded_lengths[..25].chunks(5).map(dec).sum::<usize>() != dec(&encoded_lengths[25..]) {
            return Err(eyre!("encoded_lengths fields do not match total"));
        }
        Ok(Data {
            encoded_lengths,
            dynamic_data,
            static_data,
        })
    }

    pub fn get_dynamic(&self, field: usize) -> Option<&[u8]> {
        let start_length = 20 - field * 5;
        let data_length = dec(&self.encoded_lengths[start_length..start_length + 5]);
        if data_length == 0 {
            return None;
        }
        let start = self.encoded_lengths[..25]
            .chunks(5)
            .rev()
            .take(field)
            .fold(0, |length, bytes| length + dec(bytes));
        return self.dynamic_data.get(start..start + data_length);
    }

    pub fn get_static32(&self, field: usize) -> B256 {
        let start = field * 32;
        let end = start + 32;
        B256::from_slice(self.static_data.get(start..end).unwrap())
    }
}

#[cfg(test)]
mod dynamic_data_test {
    use super::Data;
    use alloy::primitives::{fixed_bytes, FixedBytes};

    #[test]
    fn test_new_error() {
        let sd = &[];
        let el = fixed_bytes!("0000000000000000000000000000000000000000000000000000000000000020");
        let dd = &[1u8; 32];
        let dd = Data::new(sd, el, dd);
        assert!(dd.is_err());
    }

    #[test]
    fn test_new_empty() {
        let sd = &[];
        let el = fixed_bytes!("0000000000000000000000000000000000000000000000000000000000000000");
        let dd = &[];
        let dd = Data::new(sd, el, dd);
        assert!(dd.is_ok());

        let dd = dd.unwrap();
        assert!(dd.get_dynamic(0).is_none());
        assert!(dd.get_dynamic(1).is_none());
        assert!(dd.get_dynamic(2).is_none());
        assert!(dd.get_dynamic(3).is_none());
        assert!(dd.get_dynamic(4).is_none());
    }

    #[test]
    fn test_new_not_empty() {
        let sd = &[];
        let mut el = FixedBytes::<32>::ZERO;
        el[19] = 127;
        el[24] = 128;
        el[31] = 255;

        let mut dd = [0u8; 255];
        dd[..128].fill(0x01);
        dd[128..].fill(0x02);

        let dd = Data::new(sd, el, &dd);
        assert!(dd.is_ok());

        let dd = dd.unwrap();
        assert!(dd.get_dynamic(0).is_some());
        assert!(dd.get_dynamic(1).is_some());
        assert!(dd.get_dynamic(2).is_none());
        assert!(dd.get_dynamic(4).is_none());
        assert!(dd.get_dynamic(4).is_none());

        assert_eq!(dd.get_dynamic(0).unwrap(), &[1u8; 128]);
        assert_eq!(dd.get_dynamic(1).unwrap(), &[2u8; 127]);
    }
}

#[cfg(test)]
mod pl_pgsql_test {
    use crate::test_utils;
    use alloy::primitives::fixed_bytes;

    #[tokio::test]
    async fn test_ddec_empty() {
        let (_pg_server, pg) = test_utils::test_pg().await;
        let encoded_lengths =
            fixed_bytes!("0000000000000000000000000000000000000000000000000000000000000000");
        let dynamic_data = &[0u8; 0];
        let row = pg
            .query_one("select ddec($1, $2, 0)", &[&encoded_lengths, &dynamic_data])
            .await
            .expect("issue with query");
        let res: &[u8] = row.get(0);
        assert_eq!(&[0u8; 0], res)
    }

    #[tokio::test]
    async fn test_ddec() {
        let (_pg_server, pg) = test_utils::test_pg().await;
        let encoded_lengths =
            fixed_bytes!("0000000000000000000000000000000000000020000000004000000000000060");
        let dynamic_data = &[1u8; 96];
        let row = pg
            .query_one("select ddec($1, $2, 0)", &[&encoded_lengths, &dynamic_data])
            .await
            .expect("issue with query");
        let res: &[u8] = row.get(0);
        assert_eq!(&[1u8; 64], res);

        let row = pg
            .query_one("select ddec($1, $2, 1)", &[&encoded_lengths, &dynamic_data])
            .await
            .expect("issue with query");
        let res: &[u8] = row.get(0);
        assert_eq!(&[1u8; 32], res)
    }

    #[tokio::test]
    async fn test_ddec_mismatched_encoded_lengths() {
        let (_pg_server, pg) = test_utils::test_pg().await;
        let encoded_lengths_empty =
            fixed_bytes!("0000000000000000000000000000000000000000000000000000000000000000");
        let dynamic_data = &[1u8; 128];
        let row = pg
            .query_one(
                "select ddec($1, $2, 0)",
                &[&encoded_lengths_empty, &dynamic_data],
            )
            .await
            .expect("issue with query");
        let res: &[u8] = row.get(0);
        assert_eq!(&[0u8; 0], res);

        let encoded_lengths_mismatched =
            fixed_bytes!("0000000000000000000000000000000000000020000000004000000000000060");
        let row = pg
            .query_one(
                "select ddec($1, $2, 0)",
                &[&encoded_lengths_mismatched, &dynamic_data],
            )
            .await
            .expect("issue with query");

        let res: &[u8] = row.get(0);
        assert_eq!(&[1u8; 64], res)
    }
}
