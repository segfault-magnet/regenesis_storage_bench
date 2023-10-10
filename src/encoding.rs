use bincode::config::{Config, Configuration, LittleEndian, NoLimit, Varint};

use crate::serde_types::StateEntry;

pub fn encode_json<T: IntoIterator<Item = StateEntry>, K: std::io::Write>(
    entries: T,
    mut writer: K,
) {
    for entry in entries {
        serde_json::to_writer(&mut writer, &entry).unwrap();
        writer.write_all("\n".as_bytes()).unwrap();
    }
}

pub fn decode_json(mut data: impl std::io::BufRead) {
    let mut line = String::new();
    while data.read_line(&mut line).is_ok() && !line.is_empty() {
        serde_json::from_str::<StateEntry>(&line).unwrap();
        line.clear();
    }
}
pub fn encode_bson<T: IntoIterator<Item = StateEntry>, K: std::io::Write>(
    entries: T,
    mut writer: K,
) {
    for entry in entries {
        let bytes = bson::to_vec(&entry).unwrap();
        writer.write_all(&bytes).unwrap();
    }
}
pub fn decode_bson(mut data: impl std::io::BufRead) {
    while !data.fill_buf().unwrap().is_empty() {
        bson::from_reader::<_, StateEntry>(&mut data).unwrap();
    }
}

pub fn encode_bincode<T: IntoIterator<Item = StateEntry>, K: std::io::Write>(
    entries: T,
    mut writer: K,
) {
    for entry in entries {
        bincode::serde::encode_into_std_write::<
            StateEntry,
            Configuration<LittleEndian, Varint, NoLimit>,
            _,
        >(entry, &mut writer, Configuration::default())
        .unwrap();
    }
}

pub fn decode_bincode(mut data: impl std::io::BufRead) {
    while !data.fill_buf().unwrap().is_empty() {
        bincode::serde::decode_from_std_read::<
            StateEntry,
            Configuration<LittleEndian, Varint, NoLimit>,
            _,
        >(&mut data, Configuration::default())
        .unwrap();
    }
}
