use crate::serde_types::StateEntry;

pub fn encode_json<'a, T: IntoIterator<Item = &'a StateEntry>, K: std::io::Write>(
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
