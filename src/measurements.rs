use std::{
    fs::File,
    io::{BufReader, BufWriter, Seek, Write},
    path::Path,
    time::{Duration, Instant},
};

use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use itertools::Itertools;
use linregress::{FormulaRegressionBuilder, RegressionDataBuilder};

use crate::{
    encoding::{decode_json, encode_json},
    serde_types::StateEntry,
    util::payload,
};

struct TimeAndStorage {
    bytes: usize,
    encode_time: Duration,
    decode_time: Duration,
}

pub struct EncodeMeasurement {
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

pub trait LinearRegression {
    type Measurement;
    fn linear_regression(&self, start: usize, step: usize, end: usize) -> Vec<Self::Measurement>;
}

impl LinearRegression for Vec<SeekMeasurement> {
    type Measurement = SeekMeasurement;

    fn linear_regression(&self, start: usize, step: usize, end: usize) -> Vec<Self::Measurement> {
        let x = self.iter().map(|m| m.num_elements as f64).collect_vec();
        let regress = move |extract_y: fn(&SeekMeasurement) -> f64| {
            let y = self.iter().map(extract_y).collect_vec();
            gen_lin_function(x.clone(), y)
        };

        let params = [
            regress(|m| m.normal.as_secs_f64()),
            regress(|m| m.compressed.as_secs_f64()),
        ];

        (start..end)
            .step_by(step)
            .map(|num_elements| SeekMeasurement {
                num_elements,
                normal: Duration::from_secs_f64(no_negatives(params[0](num_elements))),
                compressed: Duration::from_secs_f64(no_negatives(params[1](num_elements))),
            })
            .collect()
    }
}

fn gen_lin_function(x: Vec<f64>, y: Vec<f64>) -> impl Fn(usize) -> f64 {
    let data = RegressionDataBuilder::new()
        .build_from(vec![("Y", y), ("X", x)])
        .unwrap();
    let model = FormulaRegressionBuilder::new()
        .data(&data)
        .formula("Y ~ X")
        .fit()
        .unwrap();
    let params = model.parameters();
    let (b, a) = (params[0], params[1]);

    move |x: usize| (a * x as f64 + b)
}

fn no_negatives(val: f64) -> f64 {
    if val < 0f64 {
        0f64
    } else {
        val
    }
}

impl LinearRegression for Vec<EncodeMeasurement> {
    type Measurement = EncodeMeasurement;
    fn linear_regression(&self, start: usize, step: usize, end: usize) -> Vec<Self::Measurement> {
        let x = self.iter().map(|m| m.num_elements as f64).collect_vec();
        let regress = |extract_y: fn(&EncodeMeasurement) -> f64| {
            let y = self.iter().map(extract_y).collect_vec();
            gen_lin_function(x.clone(), y)
        };

        let params = [
            regress(|m| m.normal_encode.bytes as f64),
            regress(|m| m.normal_encode.encode_time.as_secs_f64()),
            regress(|m| m.normal_encode.decode_time.as_secs_f64()),
            regress(|m| m.compressed_encode.bytes as f64),
            regress(|m| m.compressed_encode.encode_time.as_secs_f64()),
            regress(|m| m.compressed_encode.decode_time.as_secs_f64()),
        ];

        (start..end)
            .step_by(step)
            .map(|num_elements| EncodeMeasurement {
                num_elements,
                normal_encode: TimeAndStorage {
                    bytes: no_negatives(params[0](num_elements)) as usize,
                    encode_time: Duration::from_secs_f64(no_negatives(params[1](num_elements))),
                    decode_time: Duration::from_secs_f64(no_negatives(params[2](num_elements))),
                },
                compressed_encode: TimeAndStorage {
                    bytes: no_negatives(params[3](num_elements)) as usize,
                    encode_time: Duration::from_secs_f64(no_negatives(params[4](num_elements))),
                    decode_time: Duration::from_secs_f64(no_negatives(params[5](num_elements))),
                },
            })
            .collect()
    }
}

impl ToCsv for EncodeMeasurement {
    fn to_csv(&self, mut writer: impl Write) {
        let mut encode_row = move |encoding_measurement: &TimeAndStorage, compressed| {
            writer
                .write_all(
                    format!(
                        "{},{},{compressed},{},{}\n",
                        self.num_elements,
                        encoding_measurement.bytes,
                        encoding_measurement.encode_time.as_nanos(),
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
            "elements",
            "bytes",
            "compressed",
            "encode_time",
            "decode_time",
        ]
        .map(|e| e.to_string())
        .to_vec()
    }
}

fn measure_json_normal(mut buffer: &mut Vec<u8>, entries: &[StateEntry]) -> TimeAndStorage {
    buffer.clear();
    let encode_time = track_time(|| encode_json(entries, &mut buffer));
    let bytes = buffer.len();
    let decode_time = decode_normal(buffer, |buf| decode_json(buf));
    TimeAndStorage {
        bytes,
        encode_time,
        decode_time,
    }
}

fn measure_json_compressed(buffer: &mut Vec<u8>, entries: &[StateEntry]) -> TimeAndStorage {
    buffer.clear();
    let encode_time = encode_compressed(buffer, |compressor| encode_json(entries, compressor));
    let bytes = buffer.len();
    let decode_time = decode_compressed(buffer, |reader| decode_json(reader));

    TimeAndStorage {
        bytes,
        encode_time,
        decode_time,
    }
}

pub fn measure_json(buffer: &mut Vec<u8>, entries: &[StateEntry]) -> EncodeMeasurement {
    EncodeMeasurement {
        num_elements: entries.len(),
        normal_encode: measure_json_normal(buffer, entries),
        compressed_encode: measure_json_compressed(buffer, entries),
    }
}

impl<'a, T: IntoIterator<Item = &'a K>, K: ToCsv + 'a> CollectToCsv for T {
    fn collect_csv(self, mut writer: impl Write) {
        let headers = K::headers().join(",") + "\n";
        writer.write_all(headers.as_bytes()).unwrap();
        for el in self.into_iter() {
            el.to_csv(&mut writer)
        }
    }
}

pub struct SeekMeasurement {
    num_elements: usize,
    normal: Duration,
    compressed: Duration,
}

impl ToCsv for SeekMeasurement {
    fn headers() -> Vec<String> {
        ["elements", "compressed", "time"]
            .map(|e| e.to_string())
            .to_vec()
    }

    fn to_csv(&self, mut writer: impl Write) {
        let mut encode_row = move |compressed, time: Duration| {
            writer
                .write_all(
                    format!("{},{compressed},{}\n", self.num_elements, time.as_nanos()).as_bytes(),
                )
                .unwrap();
        };

        encode_row(false, self.normal);
        encode_row(true, self.compressed);
    }
}

pub fn measure_json_seek(entries: &[StateEntry]) -> SeekMeasurement {
    let normal = seek_end_uncompressed(entries);

    let compressed = seek_end_compressed(entries);
    SeekMeasurement {
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

pub struct MeasurementRunner {
    step: usize,
    entries: Vec<StateEntry>,
    buffer: Vec<u8>,
}

impl MeasurementRunner {
    pub fn new(max: usize, step: usize) -> Self {
        let entries = payload(max);
        let mut buffer = Vec::new();
        buffer.reserve(5_000_000_000);
        Self {
            entries,
            buffer,
            step,
        }
    }

    pub fn run<T>(&mut self, action: fn(&mut Vec<u8>, &[StateEntry]) -> T) -> Vec<T> {
        self.buffer.clear();
        (0..self.entries.len())
            .step_by(self.step)
            .skip(1)
            .map(|upper| &self.entries[..upper])
            .map(|entries| action(&mut self.buffer, entries))
            .collect()
    }
}
