use serde::{de::DeserializeOwned, Serialize};

use super::{Decode, Encode};
#[derive(Clone)]
pub struct JsonCodec;
impl<T: Serialize, W: std::io::Write> Encode<T, W> for JsonCodec {
    fn encode_subset(&self, data: Vec<T>, mut writer: &mut W) {
        for entry in data {
            serde_json::to_writer(&mut writer, &entry).unwrap();
            writer.write_all("\n".as_bytes()).unwrap();
        }
    }
}
impl<T: DeserializeOwned, R: std::io::BufRead> Decode<T, R> for JsonCodec {
    fn decode_subset(&self, mut data: R) {
        let mut line = String::new();
        while data.read_line(&mut line).is_ok() && !line.is_empty() {
            serde_json::from_str::<T>(&line).unwrap();
            line.clear();
        }
    }
}
