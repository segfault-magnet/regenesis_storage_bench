// pub mod api;
pub mod encoding;
pub mod measurements;
pub mod serde_types;
pub mod util;

use std::{iter::zip, path::Path};

use encoding::{BincodeCodec, ParquetCodec};
use itertools::Itertools;
use measurements::{EncodeMeasurement, LinearRegression, MeasurementRunner};
use plotters::{
    prelude::{ChartBuilder, Circle, IntoDrawingArea, PathElement, SVGBackend},
    series::{LineSeries, PointSeries},
    style::{Color, IntoFont, RGBColor, WHITE},
};
use rand::Rng;

#[derive(Debug, Copy, Clone)]
enum Shape {
    Line,
    Circle,
}

#[derive(Debug, Clone)]
struct PlotSettings {
    label: String,
    color: (u8, u8, u8),
    shape: Shape,
}

impl PlotSettings {
    pub fn normal(label: &str) -> Self {
        let mut rng = rand::thread_rng();
        Self {
            label: label.to_string(),
            color: (
                rng.gen_range(0..65),
                rng.gen_range(0..65),
                rng.gen_range(0..65),
            ),
            shape: Shape::Circle,
        }
    }
    pub fn predicted(label: &str) -> Self {
        Self {
            label: label.to_string(),
            color: (rand::random(), rand::random(), rand::random()),
            shape: Shape::Line,
        }
    }
}

fn draw_measurements(
    title: &str,
    x_desc: &str,
    y_desc: &str,
    measurement_sets: Vec<(Vec<(f64, f64)>, PlotSettings)>,
    path: impl AsRef<Path>,
) -> anyhow::Result<()> {
    let max_x = measurement_sets
        .iter()
        .flat_map(|m| &m.0)
        .map(|m| m.0)
        .max_by(|a, b| a.total_cmp(b))
        .unwrap();

    let max_y = measurement_sets
        .iter()
        .flat_map(|m| &m.0)
        .map(|m| m.1)
        .max_by(|a, b| a.total_cmp(b))
        .unwrap();

    let root = SVGBackend::new(path.as_ref(), (1980, 1200)).into_drawing_area();
    root.fill(&WHITE)?;

    let mut chart = ChartBuilder::on(&root)
        .x_label_area_size(70)
        .y_label_area_size(70)
        .margin(5)
        .caption(title, ("sans-serif", 50.0).into_font())
        .build_cartesian_2d(0f64..max_x, 0f64..max_y)?;

    chart
        .configure_mesh()
        .x_desc(x_desc)
        .y_desc(y_desc)
        .x_labels(50)
        .y_labels(50)
        .draw()?;

    for (data, details) in measurement_sets {
        let color = RGBColor(details.color.0, details.color.1, details.color.2);
        if let Shape::Circle = details.shape {
            chart
                .draw_series(PointSeries::<_, _, Circle<_, _>, _>::new(
                    data.iter().copied(),
                    3,
                    color.clone().filled(),
                ))?
                .label(&details.label)
                .legend(move |(x, y)| Circle::new((x + 10, y), 3, color.clone().filled()));
        } else {
            chart
                .draw_series(LineSeries::new(data.iter().copied(), color))?
                .label(&details.label)
                .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], color));
        }

        chart
            .configure_series_labels()
            .background_style(RGBColor(128, 128, 128))
            .draw()?;
    }

    // To avoid the IO failure being ignored silently, we manually call the present function
    root.present().expect("Unable to write result to file");

    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
#[allow(dead_code)]
enum Scale {
    #[default]
    M,
    G,
    T,
}

impl Scale {
    pub fn divider(&self) -> f64 {
        match self {
            Scale::M => 1_000_000f64,
            Scale::G => 1_000_000_000f64,
            Scale::T => 1_000_000_000_000f64,
        }
    }
    pub fn label(&self) -> &'static str {
        match self {
            Scale::M => "M",
            Scale::G => "G",
            Scale::T => "T",
        }
    }
}

#[derive(Debug, Default)]
struct PlotMerger {
    storage_scale: Scale,
    x_scale: Scale,
    bytes: Vec<(Vec<(f64, f64)>, PlotSettings)>,
    encode_time: Vec<(Vec<(f64, f64)>, PlotSettings)>,
    decode_time: Vec<(Vec<(f64, f64)>, PlotSettings)>,
}

impl PlotMerger {
    pub fn new(storage_scale: Scale, x_scale: Scale) -> Self {
        Self {
            storage_scale,
            x_scale,
            ..Default::default()
        }
    }

    pub fn add(&mut self, settings: PlotSettings, measurement: &[EncodeMeasurement]) -> &mut Self {
        let x_axis = measurement
            .iter()
            .map(|m| m.num_elements as f64 / self.x_scale.divider())
            .collect_vec();

        let bytes = measurement
            .iter()
            .map(|m| m.bytes as f64 / self.storage_scale.divider());
        self.bytes
            .push((zip(x_axis.clone(), bytes).collect(), settings.clone()));

        let encode_time = measurement.iter().map(|m| m.encode_time.as_secs_f64());
        self.encode_time
            .push((zip(x_axis.clone(), encode_time).collect(), settings.clone()));

        let decode_time = measurement.iter().map(|m| m.decode_time.as_secs_f64());
        self.decode_time
            .push((zip(x_axis, decode_time).collect(), settings.clone()));

        self
    }

    pub fn plot(self, dir: impl AsRef<Path>) -> anyhow::Result<()> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;

        draw_measurements(
            "storage requirements",
            &format!("{} elements", self.x_scale.label()),
            &format!("{}Bs", self.storage_scale.label()),
            self.bytes,
            dir.join("storage_requirements.svg"),
        )?;

        draw_measurements(
            "encoding time",
            &format!("{} elements", self.x_scale.label()),
            "s",
            self.encode_time,
            dir.join("encoding_time.svg"),
        )?;
        draw_measurements(
            "decoding time",
            &format!("{} elements", self.x_scale.label()),
            "s",
            self.decode_time,
            dir.join("decoding_time.svg"),
        )?;

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let mut measurement_runner = MeasurementRunner::new(200_000, 10_000);
    let prediction_storage_scale = Scale::G;
    let prediction_x_scale = Scale::M;

    let prediction_max = 1_000_000_000usize;
    let prediction_step = prediction_max;
    let prediction_start = 0usize;

    let parquet_codec = ParquetCodec::new(50000, 0);
    let parquet_codec_w_compression = ParquetCodec::new(50000, 1);

    // let normal_json = measurement_runner.run(&JsonCodec);
    // let normal_bson = measurement_runner.run(&BsonCodec);
    let normal_bincode = measurement_runner.run(&BincodeCodec);
    let normal_parquet = measurement_runner.run(&parquet_codec);
    let mut merger = PlotMerger::new(Scale::M, Scale::M);
    // merger.add(PlotSettings::normal("serde_json"), &normal_json);
    merger.add(PlotSettings::normal("bincode"), &normal_bincode);
    // merger.add(PlotSettings::normal("bson"), &normal_bson);
    merger.add(PlotSettings::normal("parquet"), &normal_parquet);
    merger.plot("normal")?;

    // let normal_json_predicted =
    //     normal_json.linear_regression(prediction_start, prediction_step, prediction_max);
    // let normal_bson_predicted =
    //     normal_bson.linear_regression(prediction_start, prediction_step, prediction_max);
    let normal_bincode_predicted =
        normal_bincode.linear_regression(prediction_start, prediction_step, prediction_max);
    let normal_parquet_predicted =
        normal_parquet.linear_regression(prediction_start, prediction_step, prediction_max);
    let mut merger = PlotMerger::new(prediction_storage_scale, prediction_x_scale);
    // merger.add(
    //     PlotSettings::predicted("serde_json"),
    //     &normal_json_predicted,
    // );
    merger.add(
        PlotSettings::predicted("parquet"),
        &normal_parquet_predicted,
    );
    // merger.add(PlotSettings::predicted("bson"), &normal_bson_predicted);
    merger.add(
        PlotSettings::predicted("bincode"),
        &normal_bincode_predicted,
    );
    merger.plot("normal_predicted")?;

    // let json_compressed = measurement_runner.run_compressed(&JsonCodec);
    // let bson_compressed = measurement_runner.run_compressed(&BsonCodec);
    let bincode_compressed = measurement_runner.run_compressed(&BincodeCodec);
    let parquet_compressed = measurement_runner.run(&parquet_codec_w_compression);
    let mut merger = PlotMerger::default();
    // merger.add(PlotSettings::normal("serde_json"), &json_compressed);
    merger.add(PlotSettings::normal("parquet"), &parquet_compressed);
    // merger.add(PlotSettings::normal("bson"), &bson_compressed);
    merger.add(PlotSettings::normal("bincode"), &bincode_compressed);
    merger.plot("compressed")?;

    // let json_compressed_predicted =
    //     json_compressed.linear_regression(prediction_start, prediction_step, prediction_max);
    // let bson_compressed_predicted =
    //     bson_compressed.linear_regression(prediction_start, prediction_step, prediction_max);
    let bincode_compressed_predicted =
        bincode_compressed.linear_regression(prediction_start, prediction_step, prediction_max);
    let parquet_compressed_predicted =
        parquet_compressed.linear_regression(prediction_start, prediction_step, prediction_max);
    let mut merger = PlotMerger::new(prediction_storage_scale, prediction_x_scale);
    // merger.add(
    //     PlotSettings::predicted("serde_json_compressed"),
    //     &json_compressed_predicted,
    // );
    merger.add(
        PlotSettings::predicted("bincode_compressed"),
        &bincode_compressed_predicted,
    );
    merger.add(
        PlotSettings::predicted("bincode"),
        &normal_bincode_predicted,
    );
    merger.add(
        PlotSettings::predicted("parquet_compressed"),
        &parquet_compressed_predicted,
    );
    merger.add(
        PlotSettings::predicted("parquet"),
        &normal_parquet_predicted,
    );
    // merger.add(
    //     PlotSettings::predicted("bson_compressed"),
    //     &bson_compressed_predicted,
    // );
    // merger.add(
    //     PlotSettings::predicted("serde_json"),
    //     &normal_json_predicted,
    // );
    // merger.add(PlotSettings::predicted("bson"), &normal_bson_predicted);
    merger.plot("compressed_predicted")?;

    Ok(())
}
