use serde::{de::DeserializeOwned, Serialize};

use super::{Decode, Encode};
#[derive(Clone)]
pub struct BsonCodec;
impl<T: Serialize, W: std::io::Write> Encode<T, W> for BsonCodec {
    fn encode_subset(&self, data: Vec<T>, writer: &mut W) {
        for entry in data {
            let bytes = bson::to_vec(&entry).unwrap();
            writer.write_all(&bytes).unwrap();
        }
    }
}
impl<T: DeserializeOwned, R: std::io::BufRead> Decode<T, R> for BsonCodec {
    fn decode_subset(&self, mut data: R) {
        while !data.fill_buf().unwrap().is_empty() {
            bson::from_reader::<_, T>(&mut data).unwrap();
        }
    }
}
