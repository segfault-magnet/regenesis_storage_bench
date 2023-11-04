use bytes::Bytes;
use fuel_types::{BlockHeight, Bytes32, ContractId, Salt};
use itertools::Itertools;
use parquet::{
    basic::Repetition,
    data_type::{ByteArray, ByteArrayType, FixedLenByteArrayType, Int32Type, Int64Type},
    file::{
        reader::FileReader,
        serialized_reader::SerializedFileReader,
        writer::{SerializedColumnWriter, SerializedFileWriter},
    },
    record::Field,
    schema::types::Type,
};
use std::{io::Cursor, sync::Arc};

use bincode::config::{Configuration, LittleEndian, NoLimit, Varint};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    serde_types::{CoinConfig, ContractBalance, ContractConfig, ContractState, MessageConfig},
    util::{Data, Payload},
};

pub trait PayloadCodec<R, W>:
    Code<CoinConfig, W>
    + Decode<CoinConfig, R>
    + Code<ContractConfig, W>
    + Decode<ContractConfig, R>
    + Code<MessageConfig, W>
    + Decode<MessageConfig, R>
    + Code<ContractState, W>
    + Decode<ContractState, R>
    + Code<ContractBalance, W>
    + Decode<ContractBalance, R>
{
    fn encode(&self, payload: Payload, writers: &mut Data<W>) {
        // self.encode_subset(payload.coins, &mut writers.coins);
        // self.encode_subset(payload.messages, &mut writers.messages);
        self.encode_subset(payload.contracts, &mut writers.contracts);
        // self.encode_subset(payload.contract_state, &mut writers.contract_state);
        // self.encode_subset(payload.contract_balance, &mut writers.contract_balance);
    }

    fn decode(&self, readers: Data<R>) {
        // Decode::<CoinConfig, _>::decode_subset(self, readers.coins);
        // Decode::<MessageConfig, _>::decode_subset(self, readers.messages);
        Decode::<ContractConfig, _>::decode_subset(self, readers.contracts);
        // Decode::<ContractState, _>::decode_subset(self, readers.contract_state);
        // Decode::<ContractBalance, _>::decode_subset(self, readers.contract_balance);
    }
}
impl<
        R,
        W,
        T: Code<CoinConfig, W>
            + Decode<CoinConfig, R>
            + Code<ContractConfig, W>
            + Decode<ContractConfig, R>
            + Code<MessageConfig, W>
            + Decode<MessageConfig, R>
            + Code<ContractState, W>
            + Decode<ContractState, R>
            + Code<ContractBalance, W>
            + Decode<ContractBalance, R>,
    > PayloadCodec<R, W> for T
{
}

pub trait Code<T, W> {
    fn encode_subset(&self, data: Vec<T>, writer: &mut W);
}

pub trait Decode<T, R> {
    fn decode_subset(&self, reader: R);
}

#[derive(Clone)]
pub struct JsonCodec;
impl<T: Serialize, W: std::io::Write> Code<T, W> for JsonCodec {
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

#[derive(Clone)]
pub struct BsonCodec;
impl<T: Serialize, W: std::io::Write> Code<T, W> for BsonCodec {
    fn encode_subset(&self, data: Vec<T>, writer: &mut W) {
        for entry in data {
            let bytes = bson::to_vec(&entry).unwrap();
            writer.write_all(&bytes).unwrap();
        }
    }
}
impl<T: DeserializeOwned, R: std::io::BufRead> Decode<T, R> for BsonCodec {
    fn decode_subset(&self, mut data: R) {
        while !data.fill_buf().unwrap().is_empty() {
            bson::from_reader::<_, T>(&mut data).unwrap();
        }
    }
}

#[derive(Clone)]
pub struct BincodeCodec;
impl<T: Serialize, W: std::io::Write> Code<T, W> for BincodeCodec {
    fn encode_subset(&self, data: Vec<T>, mut writer: &mut W) {
        for entry in data {
            bincode::serde::encode_into_std_write::<
                _,
                Configuration<LittleEndian, Varint, NoLimit>,
                _,
            >(entry, &mut writer, Configuration::default())
            .unwrap();
        }
    }
}

impl<T: DeserializeOwned, R: std::io::BufRead> Decode<T, R> for BincodeCodec {
    fn decode_subset(&self, mut data: R) {
        while !data.fill_buf().unwrap().is_empty() {
            bincode::serde::decode_from_std_read::<
                T,
                Configuration<LittleEndian, Varint, NoLimit>,
                _,
            >(&mut data, Configuration::default())
            .unwrap();
        }
    }
}

pub struct ParquetCodec {
    pub batch_size: usize,
}

impl ParquetCodec {
    pub fn new(batch_size: usize) -> Self {
        Self { batch_size }
    }
}

impl<W: std::io::Write + Send> Code<CoinConfig, W> for ParquetCodec {
    fn encode_subset(&self, data: Vec<CoinConfig>, writer: &mut W) {
        todo!()
    }
}
impl Decode<CoinConfig, Cursor<Vec<u8>>> for ParquetCodec {
    fn decode_subset(&self, reader: Cursor<Vec<u8>>) {
        todo!()
    }
}

impl<W: std::io::Write + Send> Code<MessageConfig, W> for ParquetCodec {
    fn encode_subset(&self, data: Vec<MessageConfig>, writer: &mut W) {
        todo!()
    }
}
impl Decode<MessageConfig, Cursor<Vec<u8>>> for ParquetCodec {
    fn decode_subset(&self, reader: Cursor<Vec<u8>>) {
        todo!()
    }
}
impl<W: std::io::Write + Send> Code<ContractState, W> for ParquetCodec {
    fn encode_subset(&self, data: Vec<ContractState>, writer: &mut W) {
        todo!()
    }
}
impl Decode<ContractState, Cursor<Vec<u8>>> for ParquetCodec {
    fn decode_subset(&self, reader: Cursor<Vec<u8>>) {
        todo!()
    }
}
impl<W: std::io::Write + Send> Code<ContractBalance, W> for ParquetCodec {
    fn encode_subset(&self, data: Vec<ContractBalance>, writer: &mut W) {
        todo!()
    }
}
impl Decode<ContractBalance, Cursor<Vec<u8>>> for ParquetCodec {
    fn decode_subset(&self, reader: Cursor<Vec<u8>>) {
        todo!()
    }
}

impl<W: std::io::Write + Send> Code<ContractConfig, W> for ParquetCodec {
    fn encode_subset(&self, data: Vec<ContractConfig>, writer: &mut W) {
        let mut writer =
            SerializedFileWriter::new(writer, Arc::new(contract_schema()), Default::default())
                .unwrap();
        for chunk in data.into_iter().chunks(self.batch_size).into_iter() {
            let mut group = writer.next_row_group().unwrap();
            let chunk = chunk.collect_vec();

            let mut column = group.next_column().unwrap().unwrap();
            let data = chunk
                .iter()
                .map(|el| el.contract_id.to_vec().into())
                .collect_vec();
            column
                .typed::<FixedLenByteArrayType>()
                .write_batch(&data, None, None)
                .unwrap();
            column.close().unwrap();

            let mut column = group.next_column().unwrap().unwrap();
            let data = chunk.iter().map(|el| el.code.clone().into()).collect_vec();
            column
                .typed::<ByteArrayType>()
                .write_batch(&data, None, None)
                .unwrap();
            column.close().unwrap();

            let mut column = group.next_column().unwrap().unwrap();
            let data = chunk.iter().map(|el| el.salt.to_vec().into()).collect_vec();
            column
                .typed::<FixedLenByteArrayType>()
                .write_batch(&data, None, None)
                .unwrap();
            column.close().unwrap();

            let mut column = group.next_column().unwrap().unwrap();
            let def_levels = chunk
                .iter()
                .map(|el| el.tx_id.is_some() as i16)
                .collect_vec();
            let data = chunk
                .iter()
                .filter_map(|el| el.tx_id)
                .map(|el| el.to_vec().into())
                .collect_vec();
            column
                .typed::<FixedLenByteArrayType>()
                .write_batch(&data, Some(&def_levels), None)
                .unwrap();
            column.close().unwrap();

            let mut column = group.next_column().unwrap().unwrap();
            let def_levels = chunk
                .iter()
                .map(|el| el.output_index.is_some() as i16)
                .collect_vec();
            let data = chunk
                .iter()
                .filter_map(|el| el.output_index)
                .map(|el| el as i32)
                .collect_vec();
            column
                .typed::<Int32Type>()
                .write_batch(&data, Some(&def_levels), None)
                .unwrap();
            column.close().unwrap();

            let mut column = group.next_column().unwrap().unwrap();
            let def_levels = chunk
                .iter()
                .map(|el| el.tx_pointer_block_height.is_some() as i16)
                .collect_vec();
            let data = chunk
                .iter()
                .filter_map(|el| el.tx_pointer_block_height)
                .map(|el| *el as i32)
                .collect_vec();
            column
                .typed::<Int32Type>()
                .write_batch(&data, Some(&def_levels), None)
                .unwrap();
            column.close().unwrap();

            let mut column = group.next_column().unwrap().unwrap();
            let def_levels = chunk
                .iter()
                .map(|el| el.tx_pointer_tx_idx.is_some() as i16)
                .collect_vec();
            let data = chunk
                .iter()
                .filter_map(|el| el.tx_pointer_tx_idx)
                .map(|el| el as i32)
                .collect_vec();
            column
                .typed::<Int32Type>()
                .write_batch(&data, Some(&def_levels), None)
                .unwrap();
            column.close().unwrap();

            group.close().unwrap();
        }
        writer.close().unwrap();
    }
}

impl Decode<ContractConfig, Cursor<Vec<u8>>> for ParquetCodec {
    fn decode_subset(&self, reader: Cursor<Vec<u8>>) {
        let reader = SerializedFileReader::new(Bytes::from(reader.into_inner())).unwrap();
        for row in reader.get_row_iter(None).unwrap() {
            let row: parquet::record::Row = row.unwrap();
            let mut iter = row.get_column_iter();

            let (_, Field::Bytes(contract_id)) = iter.next().unwrap() else {
                panic!("Unexpected type!");
            };
            let contract_id = ContractId::new(contract_id.data().try_into().unwrap());

            let Field::Bytes(code) = iter.next().unwrap().1 else {
                panic!("Unexpected type!");
            };
            let code = Vec::from(code.data());

            let Field::Bytes(salt) = iter.next().unwrap().1 else {
                panic!("Unexpected type!");
            };
            let salt = Salt::new(salt.data().try_into().unwrap());

            let tx_id = match iter.next().unwrap().1 {
                Field::Bytes(tx_id) => Some(tx_id),
                Field::Null => None,
                _ => panic!("Should not happen"),
            };
            let tx_id = tx_id.map(|data| Bytes32::new(data.data().try_into().unwrap()));

            let output_index = match iter.next().unwrap().1 {
                Field::UByte(output_index) => Some(*output_index),
                Field::Null => None,
                _ => panic!("Should not happen"),
            };

            let tx_pointer_block_height = match iter.next().unwrap().1 {
                Field::UInt(tx_pointer_block_height) => Some(*tx_pointer_block_height),
                Field::Null => None,
                _ => panic!("Should not happen"),
            };
            let tx_pointer_block_height = tx_pointer_block_height.map(BlockHeight::new);

            let tx_pointer_tx_idx = match iter.next().unwrap().1 {
                Field::UShort(tx_pointer_tx_idx) => Some(*tx_pointer_tx_idx),
                Field::Null => None,
                _ => panic!("Should not happen"),
            };
            let _deser = ContractConfig {
                contract_id,
                code,
                salt,
                tx_id,
                output_index,
                tx_pointer_block_height,
                tx_pointer_tx_idx,
            };
        }
    }
}

fn contract_schema() -> Type {
    use parquet::basic::Type as PhysicalType;
    let contract_id =
        Type::primitive_type_builder("contract_id", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_length(32)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();
    let code = Type::primitive_type_builder("code", PhysicalType::BYTE_ARRAY)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let salt = Type::primitive_type_builder("salt", PhysicalType::FIXED_LEN_BYTE_ARRAY)
        .with_length(32)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let tx_id = Type::primitive_type_builder("tx_id", PhysicalType::FIXED_LEN_BYTE_ARRAY)
        .with_length(32)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let output_index = Type::primitive_type_builder("output_index", PhysicalType::INT32)
        .with_converted_type(parquet::basic::ConvertedType::UINT_8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let tx_pointer_block_height =
        Type::primitive_type_builder("tx_pointer_block_height", PhysicalType::INT32)
            .with_converted_type(parquet::basic::ConvertedType::UINT_32)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let tx_pointer_tx_idx = Type::primitive_type_builder("tx_pointer_tx_idx", PhysicalType::INT32)
        .with_converted_type(parquet::basic::ConvertedType::UINT_16)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    parquet::schema::types::Type::group_type_builder("ContractConfig")
        .with_fields(
            [
                contract_id,
                code,
                salt,
                tx_id,
                output_index,
                tx_pointer_block_height,
                tx_pointer_tx_idx,
            ]
            .map(Arc::new)
            .to_vec(),
        )
        .build()
        .unwrap()
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn mememe() {
        let codec = ParquetCodec { batch_size: 10 };
        let mut buffer = vec![];
        let cc = ContractConfig::random(&mut rand::thread_rng());
        eprintln!("{cc:?}");
        codec.encode_subset(vec![cc], &mut buffer);

        Decode::<ContractConfig, _>::decode_subset(&codec, Cursor::new(buffer));
    }
}
