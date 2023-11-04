use bincode::config::{Configuration, LittleEndian, NoLimit, Varint};
use serde::{de::DeserializeOwned, Serialize};

use super::{Decode, Encode};
#[derive(Clone)]
pub struct BincodeCodec;
impl<T: Serialize, W: std::io::Write> Encode<T, W> for BincodeCodec {
    fn encode_subset(&self, data: Vec<T>, mut writer: &mut W) {
        for entry in data {
            bincode::serde::encode_into_std_write::<
                _,
                Configuration<LittleEndian, Varint, NoLimit>,
                _,
            >(entry, &mut writer, Configuration::default())
            .unwrap();
        }
    }
}

impl<T: DeserializeOwned, R: std::io::BufRead> Decode<T, R> for BincodeCodec {
    fn decode_subset(&self, mut data: R) {
        while !data.fill_buf().unwrap().is_empty() {
            bincode::serde::decode_from_std_read::<
                T,
                Configuration<LittleEndian, Varint, NoLimit>,
                _,
            >(&mut data, Configuration::default())
            .unwrap();
        }
    }
}
