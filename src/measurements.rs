use std::{
    fs::File,
    io::{BufReader, BufWriter, Seek, Write},
    path::Path,
    time::{Duration, Instant},
};

use flate2::{read::GzDecoder, write::GzEncoder, Compression};

use crate::{
    encoding::{decode_json, encode_json},
    serde_types::StateEntry,
};

struct TimeAndStorage {
    bytes: usize,
    encode_time: Duration,
    decode_time: Duration,
}

pub struct EncodeMeasurements {
    name: String,
    num_elements: usize,
    normal_encode: TimeAndStorage,
    compressed_encode: TimeAndStorage,
}

pub trait ToCsv {
    fn headers() -> Vec<String>;
    fn to_csv(&self, writer: impl Write);
}

pub trait CollectToCsv {
    fn collect_csv(self, writer: impl Write);
}

impl ToCsv for EncodeMeasurements {
    fn to_csv(&self, mut writer: impl Write) {
        let mut encode_row = move |encoding_measurement: &TimeAndStorage, compressed| {
            writer
                .write_all(
                    format!(
                        "{},{},{compressed},{},{},{}\n",
                        self.name,
                        self.num_elements,
                        encoding_measurement.encode_time.as_nanos(),
                        encoding_measurement.bytes,
                        encoding_measurement.decode_time.as_nanos()
                    )
                    .as_bytes(),
                )
                .unwrap();
        };

        encode_row(&self.normal_encode, false);
        encode_row(&self.compressed_encode, true);
    }

    fn headers() -> Vec<String> {
        [
            "format",
            "elements",
            "compressed",
            "encode_size",
            "encode_time",
            "encode_size",
            "decode_time",
        ]
        .map(|e| e.to_string())
        .to_vec()
    }
}

fn measure_json_normal(mut buffer: &mut Vec<u8>, entries: &[StateEntry]) -> TimeAndStorage {
    buffer.clear();
    TimeAndStorage {
        bytes: buffer.len(),
        encode_time: track_time(|| encode_json(entries, &mut buffer)),
        decode_time: decode_normal(buffer, |buf| decode_json(buf)),
    }
}

fn measure_json_compressed(buffer: &mut Vec<u8>, entries: &[StateEntry]) -> TimeAndStorage {
    buffer.clear();
    TimeAndStorage {
        bytes: buffer.len(),
        encode_time: encode_compressed(buffer, |compressor| encode_json(entries, compressor)),
        decode_time: decode_compressed(buffer, |reader| decode_json(reader)),
    }
}

pub fn measure_json(buffer: &mut Vec<u8>, entries: &[StateEntry]) -> EncodeMeasurements {
    EncodeMeasurements {
        name: "serde_json".to_string(),
        num_elements: entries.len(),
        normal_encode: measure_json_normal(buffer, entries),
        compressed_encode: measure_json_compressed(buffer, entries),
    }
}

impl<T: IntoIterator<Item = K>, K: ToCsv> CollectToCsv for T {
    fn collect_csv(self, mut writer: impl Write) {
        let headers = K::headers().join(",") + "\n";
        writer.write_all(headers.as_bytes()).unwrap();
        for el in self.into_iter() {
            el.to_csv(&mut writer)
        }
    }
}

pub struct SeekMeasurements {
    name: String,
    num_elements: usize,
    normal: Duration,
    compressed: Duration,
}

impl ToCsv for SeekMeasurements {
    fn headers() -> Vec<String> {
        ["name", "elements", "compressed", "time"]
            .map(|e| e.to_string())
            .to_vec()
    }

    fn to_csv(&self, mut writer: impl Write) {
        let mut encode_row = move |compressed, time: Duration| {
            writer
                .write_all(
                    format!(
                        "{},{},{compressed},{}\n",
                        self.name,
                        self.num_elements,
                        time.as_nanos()
                    )
                    .as_bytes(),
                )
                .unwrap();
        };

        encode_row(false, self.normal);
        encode_row(true, self.compressed);
    }
}

pub fn measure_json_seek(entries: &[StateEntry]) -> SeekMeasurements {
    let normal = seek_end_uncompressed(entries);

    let compressed = seek_end_compressed(entries);
    SeekMeasurements {
        name: "serde_json".to_owned(),
        num_elements: entries.len(),
        normal,
        compressed,
    }
}

fn track_time(action: impl FnOnce()) -> Duration {
    let start = Instant::now();
    action();
    Instant::now() - start
}

fn decode_normal(payload: &[u8], decoder: fn(&mut BufReader<&[u8]>)) -> Duration {
    let mut reader = BufReader::new(payload);

    track_time(move || decoder(&mut reader))
}

fn decode_compressed(data: &[u8], decoder: fn(&mut BufReader<GzDecoder<&[u8]>>)) -> Duration {
    let mut reader = BufReader::new(GzDecoder::new(data));

    track_time(move || decoder(&mut reader))
}

fn encode_compressed(
    buf: &mut Vec<u8>,
    encoder: impl FnOnce(&mut GzEncoder<&mut Vec<u8>>),
) -> Duration {
    let mut compressor = GzEncoder::new(buf, Compression::default());
    track_time(move || {
        encoder(&mut compressor);
        compressor.finish().unwrap();
    })
}

fn generate_json_uncompressed<'a>(
    payload: impl Iterator<Item = &'a StateEntry>,
    path: impl AsRef<Path>,
) {
    let file = File::create(path.as_ref()).unwrap();
    let mut writer = BufWriter::new(file);
    encode_json(payload, &mut writer);
}

fn generate_json_compressed<'a>(
    payload: impl Iterator<Item = &'a StateEntry>,
    path: impl AsRef<Path>,
) {
    let file = File::create(path.as_ref()).unwrap();
    let mut compressor = GzEncoder::new(file, Compression::default());
    encode_json(payload, &mut compressor);
    compressor.finish().unwrap();
}

fn seek_end_uncompressed<'a>(
    payload: impl IntoIterator<Item = &'a StateEntry>,
) -> std::time::Duration {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    generate_json_uncompressed(payload.into_iter(), tmp.path());
    tmp.as_file().sync_data().unwrap();

    let start = Instant::now();
    let mut file = File::open(tmp.path()).unwrap();
    file.seek(std::io::SeekFrom::End(0)).unwrap();

    let duration = Instant::now() - start;

    tmp.close().unwrap();
    duration
}

fn seek_end_compressed<'a>(
    payload: impl IntoIterator<Item = &'a StateEntry>,
) -> std::time::Duration {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    generate_json_compressed(payload.into_iter(), tmp.path());
    tmp.as_file().sync_data().unwrap();

    let start = Instant::now();
    let file = File::open(tmp.path()).unwrap();
    let mut decoder = GzDecoder::new(file);

    std::io::copy(
        &mut std::io::Read::by_ref(&mut decoder),
        &mut std::io::sink(),
    )
    .unwrap();

    let duration = Instant::now() - start;
    tmp.close().unwrap();
    duration
}
