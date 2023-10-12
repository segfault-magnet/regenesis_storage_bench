use std::{
    io::{BufRead, BufReader, BufWriter, Cursor, IntoInnerError, Read, Seek, Write},
    sync::{atomic::AtomicU64, Arc},
};

use bincode::config::{Configuration, LittleEndian, NoLimit, Varint};
use itertools::Itertools;
use serde::de::DeserializeOwned;

use crate::serde_types::CoinConfig;

/// So you don't have to work with files all the time. Useful for testing.
struct InMemorySource {
    // The encoded data inside a `Cursor`. Note this is not our cursor i.e. progress tracker, but
    // rather something rust provides so that you may mimic a file using only a Vec<u8>
    data: Cursor<Vec<u8>>,
    // also has a handy field containing the cursors of all batches encoded in `self.data`. Useful
    // for testing
    element_cursors: Vec<u64>,
}

/// So that we may keep track of how many bytes were written. Needed for `InMemorySource`.
#[derive(Debug)]
struct TrackingWriter<T: Debug> {
    writer: T,
    // because at some point inside `InMemorySource` we need to give up ownership of our
    // `TrackingWriter` but would still like to peek how many bytes are written at any one point.
    written_bytes: Arc<AtomicU64>,
}

impl<T: Debug> TrackingWriter<T> {
    pub fn new(writer: T) -> Self {
        Self {
            writer,
            written_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn written_bytes(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.written_bytes)
    }

    pub fn into_inner(self) -> T {
        self.writer
    }
}

impl<T: Write + Debug> Write for TrackingWriter<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.writer.write(buf)?;
        self.written_bytes
            .fetch_add(written as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl InMemorySource {
    pub fn new<T: serde::Serialize>(
        entries: impl IntoIterator<Item = T>,
        batch_size: usize,
    ) -> std::io::Result<Self> {
        let buffer = Cursor::new(vec![]);

        let writer = TrackingWriter::new(buffer);
        // this allows us to give up ownership of `writer` but still be able to peek inside it
        let bytes_written = writer.written_bytes();

        let mut writer = StateWriter::new(writer);
        let element_cursors = entries
            .into_iter()
            .chunks(batch_size)
            .into_iter()
            .map(|chunk| {
                // remember the starting position
                let cursor = bytes_written.load(std::sync::atomic::Ordering::Relaxed);
                writer.write_batch(chunk.collect_vec()).unwrap();
                // since `GenericWriter` has a buffered writer inside of it, it won't flush all the
                // time. This is bad for us here since we want all the data flushed to our
                // `TrackingWriter` so that it may count the bytes. We use that count to provide
                // the cursors for each batch -- useful for testing.
                writer.flush().unwrap();
                cursor
            })
            .collect();

        Ok(Self {
            // basically unpeals the writers, first we get the tracking writer, then we get the
            // Cursor we gave it. into_inner will flush so we can be sure that the final Cursor has
            // all the data. Also we did a bunch of flushing above
            data: writer.into_inner()?.into_inner(),
            element_cursors,
        })
    }

    // useful for tests so we don't have to hardcode boundaries
    pub fn batch_cursors(&self) -> &[u64] {
        &self.element_cursors
    }
}

impl Read for InMemorySource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.data.read(buf)
    }
}

impl Seek for InMemorySource {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.data.seek(pos)
    }
}

// So that we may record how much was written before we give it to `source`
struct TrackingBuffReader<T> {
    amount_read: u64,
    source: BufReader<T>,
}

impl<T: Seek> Seek for TrackingBuffReader<T> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.source.seek(pos)
    }
}

impl<T: Read> TrackingBuffReader<T> {
    pub fn new(source: T) -> Self {
        Self {
            amount_read: 0,
            source: BufReader::new(source),
        }
    }

    // unfortunately this is the best way i can find to check if there is any more data. the actual
    // `has_data_left` method is yet to be stabilized
    pub fn has_data_left(&mut self) -> std::io::Result<bool> {
        Ok(!self.source.fill_buf()?.is_empty())
    }
}

impl<T: Read> Read for TrackingBuffReader<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let amount = self.source.read(buf)?;
        self.amount_read += amount as u64;
        Ok(amount)
    }
}

struct StateReader<R> {
    source: TrackingBuffReader<R>,
}

impl<R: Read + Seek> StateReader<R> {
    pub fn new(source: R, start_cursor: u64) -> std::io::Result<Self> {
        let mut reader = TrackingBuffReader::new(source);
        reader.seek(std::io::SeekFrom::Start(start_cursor))?;
        Ok(Self { source: reader })
    }

    pub fn batch_cursor(&self) -> u64 {
        self.source.amount_read
    }

    pub fn read_batch<T: DeserializeOwned>(&mut self) -> anyhow::Result<Vec<T>> {
        let coins = if self.source.has_data_left()? {
            bincode::serde::decode_from_std_read(
                &mut self.source,
                Configuration::<LittleEndian, Varint, NoLimit>::default(),
            )?
        } else {
            vec![]
        };

        Ok(coins)
    }
}

struct StateWriter<W: Write> {
    dest: BufWriter<W>,
}

use std::fmt::Debug;
impl<W: Write + Debug> StateWriter<W> {
    pub fn new(dest: W) -> Self {
        Self {
            dest: BufWriter::new(dest),
        }
    }

    pub fn write_batch(&mut self, coins: Vec<impl serde::Serialize>) -> anyhow::Result<()> {
        bincode::serde::encode_into_std_write(
            coins,
            &mut self.dest,
            Configuration::<LittleEndian, Varint, NoLimit>::default(),
        )?;

        Ok(())
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.dest.flush()
    }

    pub fn into_inner(self) -> Result<W, IntoInnerError<BufWriter<W>>> {
        self.dest.into_inner()
    }
}

#[cfg(test)]
mod tests {
    use std::iter::repeat_with;

    use itertools::Itertools;

    use super::*;

    #[test]
    fn respects_cursor() {
        // given
        let coins = repeat_with(|| CoinConfig::random(&mut rand::thread_rng()))
            .take(100)
            .collect_vec();

        let in_mem = InMemorySource::new(coins.clone(), 10).unwrap();
        let start_element_cursor = in_mem.batch_cursors()[1];
        let mut reader = StateReader::new(in_mem, start_element_cursor).unwrap();

        // when
        let batch = reader.read_batch().unwrap();

        // then
        pretty_assertions::assert_eq!(coins[10..20], batch);
    }

    #[test]
    fn batch_smaller_if_not_enough_elements() {
        // given
        let coins = repeat_with(|| CoinConfig::random(&mut rand::thread_rng()))
            .take(5)
            .collect_vec();

        let in_mem = InMemorySource::new(coins.clone(), 10).unwrap();
        let mut reader = StateReader::new(in_mem, 0).unwrap();

        // when
        let batch = reader.read_batch().unwrap();

        // then
        pretty_assertions::assert_eq!(coins, batch);
    }

    #[test]
    fn cursor_stops_at_correct_locations() {
        // given
        let coins = repeat_with(|| CoinConfig::random(&mut rand::thread_rng()))
            .take(100)
            .collect_vec();

        let in_mem = InMemorySource::new(coins.clone(), 1).unwrap();
        let expected_cursors = in_mem.batch_cursors().to_vec();
        let mut reader = StateReader::new(in_mem, 0).unwrap();

        // when
        let cursors = repeat_with(|| {
            let cursor = reader.batch_cursor();
            reader.read_batch::<CoinConfig>().unwrap();
            cursor
        })
        .take(100)
        .collect_vec();

        // then
        pretty_assertions::assert_eq!(expected_cursors, cursors);
    }

    #[test]
    fn encodes_and_decodes() {
        // given
        let coins = repeat_with(|| CoinConfig::random(&mut rand::thread_rng()))
            .take(100)
            .collect_vec();
        let mut buffer = vec![];

        let mut writer = StateWriter::new(&mut buffer);

        // when
        writer.write_batch(coins.clone()).unwrap();

        // then
        let encoded = Cursor::new(writer.into_inner().unwrap());
        let mut reader = StateReader::new(encoded, 0).unwrap();
        assert_eq!(reader.read_batch::<CoinConfig>().unwrap(), coins);
    }

    #[test]
    fn works_with_files() {
        let file = tempfile::tempfile().unwrap();
        let coins = repeat_with(|| CoinConfig::random(&mut rand::thread_rng()))
            .take(100)
            .collect_vec();
        let mut writer = StateWriter::new(file);
        writer.write_batch(coins.clone()).unwrap();

        let mut file = writer.into_inner().unwrap();
        file.seek(std::io::SeekFrom::Start(0)).unwrap();

        let mut reader = StateReader::new(file, 0).unwrap();
        assert_eq!(reader.read_batch::<CoinConfig>().unwrap(), coins);
    }
}
