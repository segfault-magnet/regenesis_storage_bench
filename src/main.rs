pub mod encoding;
pub mod measurements;
pub mod serde_types;
pub mod util;

use measurements::{
    measure_json, measure_json_seek, CollectToCsv, LinearRegression, MeasurementRunner,
};

fn main() {
    let mut input_generator = MeasurementRunner::new(10_000, 1_000);

    let measurements = input_generator.run(measure_json);
    let file = std::fs::File::create("./json_encoding.csv").unwrap();
    measurements.collect_csv(file);

    let measurements = measurements.linear_regression(1_000, 1_000_000, 200_000_000);
    let file = std::fs::File::create("./json_encoding_predicted.csv").unwrap();
    measurements.collect_csv(file);

    let measurements = input_generator.run(|_, entries| measure_json_seek(entries));
    let file = std::fs::File::create("./seek.csv").unwrap();
    measurements.collect_csv(file);

    let measurements = measurements.linear_regression(1_000, 1_000_000, 200_000_000);
    let file = std::fs::File::create("./seek_predicted.csv").unwrap();
    measurements.collect_csv(file);
}
