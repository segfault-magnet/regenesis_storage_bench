pub mod encoding;
pub mod measurements;
pub mod serde_types;
pub mod util;

use std::io::Write;

use measurements::{measure_json, measure_json_seek, CollectToCsv, ToCsv};
use serde_types::StateEntry;
use util::payload;

struct MeasurementRunner {
    step: usize,
    entries: Vec<StateEntry>,
    buffer: Vec<u8>,
}

impl MeasurementRunner {
    fn new(max: usize, step: usize) -> Self {
        let entries = payload(max);
        let mut buffer = Vec::new();
        buffer.reserve(5_000_000_000);
        Self {
            entries,
            buffer,
            step,
        }
    }

    fn run<T: ToCsv>(&mut self, action: fn(&mut Vec<u8>, &[StateEntry]) -> T, writer: impl Write) {
        self.buffer.clear();
        (0..self.entries.len())
            .step_by(self.step)
            .map(|upper| &self.entries[..upper])
            .map(|entries| action(&mut self.buffer, entries))
            .collect_csv(writer);
    }
}

fn main() {
    let mut input_generator = MeasurementRunner::new(100_000, 1_000);

    input_generator.run(measure_json, std::io::stdout());
    input_generator.run(|_, entries| measure_json_seek(entries), std::io::stdout());
}
