use std::{io::Cursor, sync::Arc};

use bytes::Bytes;
use fuel_core_types::blockchain::primitives::DaBlockHeight;
use fuel_types::{Address, AssetId, BlockHeight, Bytes32, ContractId, Nonce, Salt};
use itertools::Itertools;
use parquet::{
    basic::{Compression, GzipLevel, Repetition},
    data_type::{ByteArrayType, FixedLenByteArrayType, Int32Type, Int64Type},
    file::{
        properties::WriterProperties,
        reader::FileReader,
        serialized_reader::SerializedFileReader,
        writer::{SerializedColumnWriter, SerializedFileWriter},
    },
    record::{Field, Row},
    schema::types::Type,
};

use super::{Decode, Encode};
use crate::serde_types::{
    CoinConfig, ContractBalance, ContractConfig, ContractState, MessageConfig,
};

trait ParquetSchema {
    fn schema() -> Type;
    fn num_of_columns() -> usize {
        Self::schema().get_fields().len()
    }
}

trait ColumnEncoder {
    type ElementT: ParquetSchema;
    fn encode_columns<W: std::io::Write + Send>(&self, writer: &mut SerializedFileWriter<W>) {
        let mut group = writer.next_row_group().unwrap();

        for index in 0..<Self::ElementT>::num_of_columns() {
            let mut column = group.next_column().unwrap().unwrap();
            self.encode_column(index, &mut column);
            column.close().unwrap();
        }

        group.close().unwrap();
    }
    fn encode_column(&self, index: usize, column: &mut SerializedColumnWriter<'_>);
}

impl ColumnEncoder for Vec<ContractConfig> {
    type ElementT = ContractConfig;

    fn encode_column(&self, index: usize, column: &mut SerializedColumnWriter<'_>) {
        match index {
            0 => {
                let data = self
                    .iter()
                    .map(|el| el.contract_id.to_vec().into())
                    .collect_vec();
                column
                    .typed::<FixedLenByteArrayType>()
                    .write_batch(&data, None, None)
                    .unwrap();
            }
            1 => {
                let data = self.iter().map(|el| el.code.clone().into()).collect_vec();
                column
                    .typed::<ByteArrayType>()
                    .write_batch(&data, None, None)
                    .unwrap();
            }
            2 => {
                let data = self.iter().map(|el| el.salt.to_vec().into()).collect_vec();
                column
                    .typed::<FixedLenByteArrayType>()
                    .write_batch(&data, None, None)
                    .unwrap();
            }
            3 => {
                let def_levels = self
                    .iter()
                    .map(|el| el.tx_id.is_some() as i16)
                    .collect_vec();
                let data = self
                    .iter()
                    .filter_map(|el| el.tx_id)
                    .map(|el| el.to_vec().into())
                    .collect_vec();
                column
                    .typed::<FixedLenByteArrayType>()
                    .write_batch(&data, Some(&def_levels), None)
                    .unwrap();
            }
            4 => {
                let def_levels = self
                    .iter()
                    .map(|el| el.output_index.is_some() as i16)
                    .collect_vec();
                let data = self
                    .iter()
                    .filter_map(|el| el.output_index)
                    .map(|el| el as i32)
                    .collect_vec();
                column
                    .typed::<Int32Type>()
                    .write_batch(&data, Some(&def_levels), None)
                    .unwrap();
            }
            5 => {
                let def_levels = self
                    .iter()
                    .map(|el| el.tx_pointer_block_height.is_some() as i16)
                    .collect_vec();
                let data = self
                    .iter()
                    .filter_map(|el| el.tx_pointer_block_height)
                    .map(|el| *el as i32)
                    .collect_vec();
                column
                    .typed::<Int32Type>()
                    .write_batch(&data, Some(&def_levels), None)
                    .unwrap();
            }
            6 => {
                let def_levels = self
                    .iter()
                    .map(|el| el.tx_pointer_tx_idx.is_some() as i16)
                    .collect_vec();
                let data = self
                    .iter()
                    .filter_map(|el| el.tx_pointer_tx_idx)
                    .map(|el| el as i32)
                    .collect_vec();
                column
                    .typed::<Int32Type>()
                    .write_batch(&data, Some(&def_levels), None)
                    .unwrap();
            }
            unknown_column => {
                panic!(
                    "Unknown column {unknown_column}, doesn't index schema: {:?}",
                    <Self::ElementT>::schema()
                )
            }
        }
    }
}
impl ColumnEncoder for Vec<CoinConfig> {
    type ElementT = CoinConfig;

    fn encode_column(&self, index: usize, column: &mut SerializedColumnWriter<'_>) {
        match index {
            0 => {
                let def_levels = self
                    .iter()
                    .map(|el| el.tx_id.is_some() as i16)
                    .collect_vec();
                let data = self
                    .iter()
                    .filter_map(|el| el.tx_id)
                    .map(|el| el.to_vec().into())
                    .collect_vec();
                column
                    .typed::<FixedLenByteArrayType>()
                    .write_batch(&data, Some(&def_levels), None)
                    .unwrap();
            }
            1 => {
                let def_levels = self
                    .iter()
                    .map(|el| el.output_index.is_some() as i16)
                    .collect_vec();
                let data = self
                    .iter()
                    .filter_map(|el| el.output_index)
                    .map(|el| el as i32)
                    .collect_vec();
                column
                    .typed::<Int32Type>()
                    .write_batch(&data, Some(&def_levels), None)
                    .unwrap();
            }
            2 => {
                let def_levels = self
                    .iter()
                    .map(|el| el.tx_pointer_block_height.is_some() as i16)
                    .collect_vec();
                let data = self
                    .iter()
                    .filter_map(|el| el.tx_pointer_block_height)
                    .map(|el| *el as i32)
                    .collect_vec();
                column
                    .typed::<Int32Type>()
                    .write_batch(&data, Some(&def_levels), None)
                    .unwrap();
            }
            3 => {
                let def_levels = self
                    .iter()
                    .map(|el| el.tx_pointer_tx_idx.is_some() as i16)
                    .collect_vec();
                let data = self
                    .iter()
                    .filter_map(|el| el.tx_pointer_tx_idx)
                    .map(|el| el as i32)
                    .collect_vec();
                column
                    .typed::<Int32Type>()
                    .write_batch(&data, Some(&def_levels), None)
                    .unwrap();
            }
            4 => {
                let def_levels = self
                    .iter()
                    .map(|el| el.maturity.is_some() as i16)
                    .collect_vec();
                let data = self
                    .iter()
                    .filter_map(|el| el.maturity)
                    .map(|el| *el as i32)
                    .collect_vec();
                column
                    .typed::<Int32Type>()
                    .write_batch(&data, Some(&def_levels), None)
                    .unwrap();
            }
            5 => {
                let data = self.iter().map(|el| el.owner.to_vec().into()).collect_vec();
                column
                    .typed::<FixedLenByteArrayType>()
                    .write_batch(&data, None, None)
                    .unwrap();
            }
            6 => {
                let data = self.iter().map(|el| el.amount as i64).collect_vec();
                column
                    .typed::<Int64Type>()
                    .write_batch(&data, None, None)
                    .unwrap();
            }
            7 => {
                let data = self
                    .iter()
                    .map(|el| el.asset_id.to_vec().into())
                    .collect_vec();
                column
                    .typed::<FixedLenByteArrayType>()
                    .write_batch(&data, None, None)
                    .unwrap();
            }
            unknown_column => {
                panic!(
                    "Unknown column {unknown_column}, doesn't index schema: {:?}",
                    <Self::ElementT>::schema()
                )
            }
        }
    }
}
impl ColumnEncoder for Vec<MessageConfig> {
    type ElementT = MessageConfig;

    fn encode_column(&self, index: usize, column: &mut SerializedColumnWriter<'_>) {
        match index {
            0 => {
                let data = self
                    .iter()
                    .map(|el| el.sender.to_vec().into())
                    .collect_vec();
                column
                    .typed::<FixedLenByteArrayType>()
                    .write_batch(&data, None, None)
                    .unwrap();
            }
            1 => {
                let data = self
                    .iter()
                    .map(|el| el.recipient.to_vec().into())
                    .collect_vec();
                column
                    .typed::<FixedLenByteArrayType>()
                    .write_batch(&data, None, None)
                    .unwrap();
            }
            2 => {
                let data = self.iter().map(|el| el.nonce.to_vec().into()).collect_vec();
                column
                    .typed::<FixedLenByteArrayType>()
                    .write_batch(&data, None, None)
                    .unwrap();
            }
            3 => {
                let data = self.iter().map(|el| el.amount as i64).collect_vec();
                column
                    .typed::<Int64Type>()
                    .write_batch(&data, None, None)
                    .unwrap();
            }
            4 => {
                let data = self.iter().map(|el| el.data.to_vec().into()).collect_vec();
                column
                    .typed::<ByteArrayType>()
                    .write_batch(&data, None, None)
                    .unwrap();
            }
            5 => {
                let data = self.iter().map(|el| el.da_height.0 as i64).collect_vec();
                column
                    .typed::<Int64Type>()
                    .write_batch(&data, None, None)
                    .unwrap();
            }
            unknown_column => {
                panic!(
                    "Unknown column {unknown_column}, doesn't index schema: {:?}",
                    <Self::ElementT>::schema()
                )
            }
        }
    }
}
impl ColumnEncoder for Vec<ContractState> {
    type ElementT = ContractState;

    fn encode_column(&self, index: usize, column: &mut SerializedColumnWriter<'_>) {
        match index {
            0 => {
                let data = self.iter().map(|el| el.key.to_vec().into()).collect_vec();
                column
                    .typed::<FixedLenByteArrayType>()
                    .write_batch(&data, None, None)
                    .unwrap();
            }
            1 => {
                let data = self.iter().map(|el| el.value.to_vec().into()).collect_vec();
                column
                    .typed::<FixedLenByteArrayType>()
                    .write_batch(&data, None, None)
                    .unwrap();
            }
            unknown_column => {
                panic!(
                    "Unknown column {unknown_column}, doesn't index schema: {:?}",
                    <Self::ElementT>::schema()
                )
            }
        }
    }
}
impl ColumnEncoder for Vec<ContractBalance> {
    type ElementT = ContractBalance;

    fn encode_column(&self, index: usize, column: &mut SerializedColumnWriter<'_>) {
        match index {
            0 => {
                let data = self
                    .iter()
                    .map(|el| el.asset_id.to_vec().into())
                    .collect_vec();
                column
                    .typed::<FixedLenByteArrayType>()
                    .write_batch(&data, None, None)
                    .unwrap();
            }
            1 => {
                let data = self.iter().map(|el| el.amount as i64).collect_vec();
                column
                    .typed::<Int64Type>()
                    .write_batch(&data, None, None)
                    .unwrap();
            }
            unknown_column => {
                panic!(
                    "Unknown column {unknown_column}, doesn't index schema: {:?}",
                    <Self::ElementT>::schema()
                )
            }
        }
    }
}

pub struct ParquetCodec {
    pub batch_size: usize,
    pub compression_level: u32,
}

impl ParquetCodec {
    pub fn new(batch_size: usize, compression_level: u32) -> Self {
        Self {
            batch_size,
            compression_level,
        }
    }
}

impl<T, W> Encode<T, W> for ParquetCodec
where
    Vec<T>: ColumnEncoder<ElementT = T>,
    T: ParquetSchema,
    W: std::io::Write + Send,
{
    fn encode_subset(&self, data: Vec<T>, writer: &mut W) {
        let mut writer = SerializedFileWriter::new(
            writer,
            Arc::new(T::schema()),
            Arc::new(
                WriterProperties::builder()
                    .set_compression(Compression::GZIP(
                        GzipLevel::try_new(self.compression_level).unwrap(),
                    ))
                    .build(),
            ),
        )
        .unwrap();
        for chunk in data.into_iter().chunks(self.batch_size).into_iter() {
            chunk.collect_vec().encode_columns(&mut writer);
        }
        writer.close().unwrap();
    }
}

impl From<Row> for CoinConfig {
    fn from(row: Row) -> Self {
        let mut iter = row.get_column_iter();

        let tx_id = match iter.next().unwrap().1 {
            Field::Null => None,
            Field::Bytes(tx_id) => Some(tx_id),
            _ => panic!("Unexpected type!"),
        };
        let tx_id = tx_id.map(|bytes| Bytes32::new(bytes.data().try_into().unwrap()));

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
        let maturity = match iter.next().unwrap().1 {
            Field::UInt(maturity) => Some(*maturity),
            Field::Null => None,
            _ => panic!("Should not happen"),
        };
        let maturity = maturity.map(BlockHeight::new);

        let Field::Bytes(owner) = iter.next().unwrap().1 else {
            panic!("Unexpected type!");
        };
        let owner = Address::new(owner.data().try_into().unwrap());

        let Field::ULong(amount) = iter.next().unwrap().1 else {
            panic!("Unexpected type!");
        };
        let amount = *amount;

        let Field::Bytes(asset_id) = iter.next().unwrap().1 else {
            panic!("Unexpected type!");
        };
        let asset_id = AssetId::new(asset_id.data().try_into().unwrap());

        Self {
            tx_id,
            output_index,
            tx_pointer_block_height,
            tx_pointer_tx_idx,
            maturity,
            owner,
            amount,
            asset_id,
        }
    }
}
impl From<Row> for MessageConfig {
    fn from(row: Row) -> Self {
        let mut iter = row.get_column_iter();

        let Field::Bytes(sender) = iter.next().unwrap().1 else {
            panic!("Unexpected type!");
        };
        let sender = Address::new(sender.data().try_into().unwrap());

        let Field::Bytes(recipient) = iter.next().unwrap().1 else {
            panic!("Unexpected type!");
        };
        let recipient = Address::new(recipient.data().try_into().unwrap());

        let Field::Bytes(nonce) = iter.next().unwrap().1 else {
            panic!("Unexpected type!");
        };
        let nonce = Nonce::new(nonce.data().try_into().unwrap());

        let Field::ULong(amount) = iter.next().unwrap().1 else {
            panic!("Unexpected type!");
        };
        let amount = *amount;

        let Field::Bytes(data) = iter.next().unwrap().1 else {
            panic!("Unexpected type!");
        };
        let data = data.data().to_vec();

        let Field::ULong(da_height) = iter.next().unwrap().1 else {
            panic!("Unexpected type!");
        };
        let da_height = DaBlockHeight(*da_height);

        Self {
            sender,
            recipient,
            nonce,
            amount,
            data,
            da_height,
        }
    }
}
impl From<Row> for ContractState {
    fn from(row: Row) -> Self {
        let mut iter = row.get_column_iter();

        let Field::Bytes(key) = iter.next().unwrap().1 else {
            panic!("Unexpected type!");
        };
        let key = Bytes32::new(key.data().try_into().unwrap());
        let Field::Bytes(value) = iter.next().unwrap().1 else {
            panic!("Unexpected type!");
        };
        let value = Bytes32::new(value.data().try_into().unwrap());

        Self { key, value }
    }
}
impl From<Row> for ContractConfig {
    fn from(row: Row) -> Self {
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
        Self {
            contract_id,
            code,
            salt,
            tx_id,
            output_index,
            tx_pointer_block_height,
            tx_pointer_tx_idx,
        }
    }
}

impl From<Row> for ContractBalance {
    fn from(row: Row) -> Self {
        let mut iter = row.get_column_iter();

        let Field::Bytes(asset_id) = iter.next().unwrap().1 else {
            panic!("Unexpected type!");
        };
        let asset_id = AssetId::new(asset_id.data().try_into().unwrap());

        let Field::ULong(amount) = iter.next().unwrap().1 else {
            panic!("Unexpected type!");
        };
        let amount = *amount;

        Self { asset_id, amount }
    }
}

impl<T> Decode<T, Cursor<Vec<u8>>> for ParquetCodec
where
    T: ParquetSchema + From<Row>,
{
    fn decode_subset(&self, reader: Cursor<Vec<u8>>) {
        let reader = SerializedFileReader::new(Bytes::from(reader.into_inner())).unwrap();
        for row in reader.get_row_iter(Some(T::schema())).unwrap() {
            let _ = T::from(row.unwrap());
        }
    }
}

impl ParquetSchema for ContractConfig {
    fn schema() -> Type {
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

        let tx_pointer_tx_idx =
            Type::primitive_type_builder("tx_pointer_tx_idx", PhysicalType::INT32)
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
}

impl ParquetSchema for ContractState {
    fn schema() -> Type {
        use parquet::basic::Type as PhysicalType;
        let key = Type::primitive_type_builder("key", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_length(32)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();
        let value = Type::primitive_type_builder("value", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_length(32)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();

        parquet::schema::types::Type::group_type_builder("ContractState")
            .with_fields([key, value].map(Arc::new).to_vec())
            .build()
            .unwrap()
    }
}

impl ParquetSchema for ContractBalance {
    fn schema() -> Type {
        use parquet::basic::Type as PhysicalType;
        let asset_id = Type::primitive_type_builder("asset_id", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_length(32)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();
        let amount = Type::primitive_type_builder("amount", PhysicalType::INT64)
            .with_converted_type(parquet::basic::ConvertedType::UINT_64)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();

        parquet::schema::types::Type::group_type_builder("ContractBalance")
            .with_fields([asset_id, amount].map(Arc::new).to_vec())
            .build()
            .unwrap()
    }
}

impl ParquetSchema for CoinConfig {
    fn schema() -> Type {
        use parquet::basic::Type as PhysicalType;
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
        let tx_pointer_tx_idx =
            Type::primitive_type_builder("tx_pointer_tx_idx", PhysicalType::INT32)
                .with_converted_type(parquet::basic::ConvertedType::UINT_16)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .unwrap();
        let maturity = Type::primitive_type_builder("maturity", PhysicalType::INT32)
            .with_converted_type(parquet::basic::ConvertedType::UINT_32)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();
        let owner = Type::primitive_type_builder("owner", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_length(32)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();
        let amount = Type::primitive_type_builder("amount", PhysicalType::INT64)
            .with_converted_type(parquet::basic::ConvertedType::UINT_64)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();
        let asset_id = Type::primitive_type_builder("asset_id", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_length(32)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();

        parquet::schema::types::Type::group_type_builder("CoinConfig")
            .with_fields(
                [
                    tx_id,
                    output_index,
                    tx_pointer_block_height,
                    tx_pointer_tx_idx,
                    maturity,
                    owner,
                    amount,
                    asset_id,
                ]
                .map(Arc::new)
                .to_vec(),
            )
            .build()
            .unwrap()
    }
}

impl ParquetSchema for MessageConfig {
    fn schema() -> Type {
        use parquet::basic::Type as PhysicalType;
        let sender = Type::primitive_type_builder("sender", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_length(32)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();
        let recipient =
            Type::primitive_type_builder("recipient", PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_length(32)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap();
        let nonce = Type::primitive_type_builder("nonce", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_length(32)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();
        let amount = Type::primitive_type_builder("amount", PhysicalType::INT64)
            .with_converted_type(parquet::basic::ConvertedType::UINT_64)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();
        let data = Type::primitive_type_builder("data", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();
        let da_height = Type::primitive_type_builder("da_height", PhysicalType::INT64)
            .with_converted_type(parquet::basic::ConvertedType::UINT_64)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();

        parquet::schema::types::Type::group_type_builder("CoinConfig")
            .with_fields(
                [sender, recipient, nonce, amount, data, da_height]
                    .map(Arc::new)
                    .to_vec(),
            )
            .build()
            .unwrap()
    }
}
