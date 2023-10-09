pub mod encoding;
pub mod measurements;
pub mod serde_types;
pub mod util;

use std::{cmp::max, path::Path};

use itertools::{chain, Itertools};
use measurements::{
    measure_bincode_compressed, measure_bincode_normal, measure_json_compressed,
    measure_json_normal, measure_json_seek, CollectToCsv, EncodeMeasurement, LinearRegression,
    MeasurementRunner,
};
use plotters::{
    prelude::{ChartBuilder, Circle, IntoDrawingArea, PathElement, SVGBackend},
    series::{LineSeries, PointSeries},
    style::{Color, IntoFont, RGBColor, BLUE, WHITE},
};
use serde::de;

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
        Self {
            label: label.to_string(),
            color: (rand::random(), rand::random(), rand::random()),
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
    y_desc: &str,
    measurement_sets: Vec<(Vec<(usize, f64)>, PlotSettings)>,
    path: impl AsRef<Path>,
) -> anyhow::Result<()> {
    let max_x = measurement_sets
        .iter()
        .flat_map(|m| &m.0)
        .map(|m| m.0)
        .max()
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
        .build_cartesian_2d(0usize..max_x, 0f64..max_y)?;

    chart
        .configure_mesh()
        .x_desc("# Elements")
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

#[derive(Debug, Default)]
struct PlotMerger {
    bytes: Vec<(Vec<(usize, f64)>, PlotSettings)>,
    encode_time: Vec<(Vec<(usize, f64)>, PlotSettings)>,
    decode_time: Vec<(Vec<(usize, f64)>, PlotSettings)>,
}

impl PlotMerger {
    pub fn add(&mut self, settings: PlotSettings, measurement: &[EncodeMeasurement]) -> &mut Self {
        let data = measurement
            .iter()
            .map(|m| (m.num_elements, m.bytes as f64 / 1_000_000f64))
            .collect();

        self.bytes.push((data, settings.clone()));

        let data = measurement
            .iter()
            .map(|m| (m.num_elements, m.encode_time.as_secs_f64()))
            .collect();
        self.encode_time.push((data, settings.clone()));

        let data = measurement
            .iter()
            .map(|m| (m.num_elements, m.decode_time.as_secs_f64()))
            .collect();
        self.decode_time.push((data, settings));

        self
    }

    pub fn plot(self, dir: impl AsRef<Path>) -> anyhow::Result<()> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;

        draw_measurements(
            "storage requirements",
            "MBs",
            self.bytes,
            dir.join("storage_requirements.svg"),
        )?;

        draw_measurements(
            "encoding time",
            "s",
            self.encode_time,
            dir.join("encoding_time.svg"),
        )?;
        draw_measurements(
            "decoding time",
            "s",
            self.decode_time,
            dir.join("decoding_time.svg"),
        )?;

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let mut measurement_runner = MeasurementRunner::new(100_000, 10_000);

    let normal_json = measurement_runner.run(measure_json_normal);
    let normal_bincode = measurement_runner.run(measure_bincode_normal);
    let mut merger = PlotMerger::default();
    merger.add(PlotSettings::normal("serde_json"), &normal_json);
    merger.add(PlotSettings::normal("bincode"), &normal_bincode);
    merger.plot("normal")?;

    let normal_json_predicted = normal_json.linear_regression(0, 1_000, 100_000_000);
    let normal_bincode_predicted = normal_bincode.linear_regression(0, 1_000, 100_000_000);
    let mut merger = PlotMerger::default();
    merger.add(
        PlotSettings::predicted("serde_json"),
        &normal_json_predicted,
    );
    merger.add(
        PlotSettings::predicted("bincode"),
        &normal_bincode_predicted,
    );
    merger.plot("normal_predicted")?;

    let json_compressed = measurement_runner.run(measure_json_compressed);
    let bincode_compressed = measurement_runner.run(measure_bincode_compressed);
    let mut merger = PlotMerger::default();
    merger.add(PlotSettings::normal("serde_json"), &json_compressed);
    merger.add(PlotSettings::normal("bincode"), &bincode_compressed);
    merger.plot("compressed")?;

    let json_compressed_predicted = json_compressed.linear_regression(0, 10_000, 100_000_000);
    let bincode_compressed_predicted = bincode_compressed.linear_regression(0, 10_000, 100_000_000);
    let mut merger = PlotMerger::default();
    merger.add(
        PlotSettings::predicted("serde_json_compressed"),
        &json_compressed_predicted,
    );
    merger.add(
        PlotSettings::predicted("bincode_compressed"),
        &bincode_compressed_predicted,
    );
    merger.add(
        PlotSettings::predicted("serde_json"),
        &normal_json_predicted,
    );
    merger.add(
        PlotSettings::predicted("bincode"),
        &normal_bincode_predicted,
    );
    merger.plot("compressed_predicted")?;

    Ok(())
}
