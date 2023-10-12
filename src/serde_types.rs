use core::fmt;
use std::{convert::TryFrom, io::Write, sync::Arc};

use fuel_core_types::{blockchain::primitives::DaBlockHeight, fuel_types::bytes::WORD_SIZE};
use fuel_types::{Address, AssetId, BlockHeight, Bytes32, ContractId, Nonce, Salt, Word};
use parquet::{
    basic::{LogicalType, Repetition},
    data_type::{ByteArray, ByteArrayType, FixedLenByteArrayType, Int32Type},
    file::writer::SerializedFileWriter,
    schema::types::Type,
};
use rand::Rng;
use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{serde_as, DeserializeAs, SerializeAs};

use crate::util::random_bytes_32;

#[serde_as]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CoinConfig {
    /// auto-generated if None
    #[serde_as(as = "Option<HexType>")]
    #[serde(default)]
    pub tx_id: Option<Bytes32>,
    #[serde_as(as = "Option<HexNumber>")]
    #[serde(default)]
    pub output_index: Option<u8>,
    /// used if coin is forked from another chain to preserve id & tx_pointer
    #[serde_as(as = "Option<HexNumber>")]
    #[serde(default)]
    pub tx_pointer_block_height: Option<BlockHeight>,
    /// used if coin is forked from another chain to preserve id & tx_pointer
    /// The index of the originating tx within `tx_pointer_block_height`
    #[serde_as(as = "Option<HexNumber>")]
    #[serde(default)]
    pub tx_pointer_tx_idx: Option<u16>,
    #[serde_as(as = "Option<HexNumber>")]
    #[serde(default)]
    pub maturity: Option<BlockHeight>,
    #[serde_as(as = "HexType")]
    pub owner: Address,
    #[serde_as(as = "HexNumber")]
    pub amount: u64,
    #[serde_as(as = "HexType")]
    pub asset_id: AssetId,
}

impl CoinConfig {
    pub fn random(rng: &mut impl Rng) -> Self {
        CoinConfig {
            tx_id: Some(random_bytes_32(rng)),
            output_index: Some(rng.gen()),
            tx_pointer_block_height: Some(BlockHeight::new(rng.gen())),
            tx_pointer_tx_idx: Some(rng.gen()),
            maturity: Some(BlockHeight::new(rng.gen())),
            owner: Address::new(*random_bytes_32(rng)),
            amount: rng.gen(),
            asset_id: AssetId::new(*random_bytes_32(rng)),
        }
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct ContractConfig {
    #[serde_as(as = "HexType")]
    pub contract_id: ContractId,
    #[serde_as(as = "HexType")]
    pub code: Vec<u8>,
    #[serde_as(as = "HexType")]
    pub salt: Salt,
    /// UtxoId: auto-generated if None
    #[serde_as(as = "Option<HexType>")]
    #[serde(default)]
    pub tx_id: Option<Bytes32>,
    pub output_index: Option<u8>,
    /// TxPointer: auto-generated if None
    /// used if contract is forked from another chain to preserve id & tx_pointer
    /// The block height that the contract was last used in
    #[serde_as(as = "Option<HexNumber>")]
    #[serde(default)]
    pub tx_pointer_block_height: Option<BlockHeight>,
    /// TxPointer: auto-generated if None
    /// used if contract is forked from another chain to preserve id & tx_pointer
    /// The index of the originating tx within `tx_pointer_block_height`
    pub tx_pointer_tx_idx: Option<u16>,
}

trait ParquetTrait {
    fn schema() -> Type;
    fn write<T: Write + Send>(self, writer: &mut SerializedFileWriter<T>);
}

impl ParquetTrait for ContractConfig {
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

    fn write<T: Write + Send>(self, writer: &mut SerializedFileWriter<T>) {
        let mut group: parquet::file::writer::SerializedRowGroupWriter<'_, T> =
            writer.next_row_group().unwrap();

        let mut column = group.next_column().unwrap().unwrap();

        let encoded = self.contract_id.to_vec().into();
        column
            .typed::<FixedLenByteArrayType>()
            .write_batch(std::slice::from_ref(&encoded), None, None)
            .unwrap();
        column.close().unwrap();
        let mut column = group.next_column().unwrap().unwrap();

        let encoded = ByteArray::from(self.code);
        column
            .typed::<ByteArrayType>()
            .write_batch(std::slice::from_ref(&encoded), None, None)
            .unwrap();

        column.close().unwrap();
        let mut column = group.next_column().unwrap().unwrap();

        let encoded = self.salt.to_vec().into();
        column
            .typed::<FixedLenByteArrayType>()
            .write_batch(std::slice::from_ref(&encoded), None, None)
            .unwrap();

        column.close().unwrap();
        let mut column = group.next_column().unwrap().unwrap();

        if let Some(encoded) = self.tx_id.map(|tx_id| tx_id.to_vec().into()) {
            column
                .typed::<FixedLenByteArrayType>()
                .write_batch(std::slice::from_ref(&encoded), Some(&[1]), None)
                .unwrap();
        }
        column.close().unwrap();
        let mut column = group.next_column().unwrap().unwrap();

        if let Some(encoded) = self.output_index {
            column
                .typed::<Int32Type>()
                .write_batch(std::slice::from_ref(&(encoded as i32)), Some(&[1]), None)
                .unwrap();
        }
        column.close().unwrap();
        let mut column = group.next_column().unwrap().unwrap();

        if let Some(encoded) = self.tx_pointer_block_height {
            column
                .typed::<Int32Type>()
                .write_batch(std::slice::from_ref(&(*encoded as i32)), Some(&[1]), None)
                .unwrap();
        }
        column.close().unwrap();
        let mut column = group.next_column().unwrap().unwrap();

        if let Some(encoded) = self.tx_pointer_tx_idx {
            column
                .typed::<Int32Type>()
                .write_batch(std::slice::from_ref(&(encoded as i32)), Some(&[1]), None)
                .unwrap();
        }
        column.close().unwrap();
        group.close().unwrap();
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct ContractState {
    #[serde_as(as = "HexType")]
    pub key: Bytes32,
    #[serde_as(as = "HexType")]
    pub value: Bytes32,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct ContractBalance {
    #[serde_as(as = "HexType")]
    pub asset_id: AssetId,
    #[serde_as(as = "HexNumber")]
    pub amount: u64,
}

impl ContractConfig {
    pub fn random(rng: &mut impl Rng) -> Self {
        ContractConfig {
            contract_id: ContractId::new(*random_bytes_32(rng)),
            code: (*random_bytes_32(rng)).to_vec(),
            salt: Salt::new(*random_bytes_32(rng)),
            tx_id: Some(random_bytes_32(rng)),
            output_index: Some(rng.gen()),
            tx_pointer_block_height: Some(BlockHeight::from(rng.gen::<u32>())),
            tx_pointer_tx_idx: Some(rng.gen()),
        }
    }
}

#[serde_as]
#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
pub struct MessageConfig {
    #[serde_as(as = "HexType")]
    pub sender: Address,
    #[serde_as(as = "HexType")]
    pub recipient: Address,
    #[serde_as(as = "HexType")]
    pub nonce: Nonce,
    #[serde_as(as = "HexNumber")]
    pub amount: Word,
    #[serde_as(as = "HexType")]
    pub data: Vec<u8>,
    /// The block height from the parent da layer that originated this message
    #[serde_as(as = "HexNumber")]
    pub da_height: DaBlockHeight,
}

impl MessageConfig {
    pub fn random(rng: &mut impl Rng) -> Self {
        MessageConfig {
            sender: Address::new(*random_bytes_32(rng)),
            recipient: Address::new(*random_bytes_32(rng)),
            nonce: Nonce::new(*random_bytes_32(rng)),
            amount: rng.gen(),
            data: (*random_bytes_32(rng)).to_vec(),
            da_height: DaBlockHeight(rng.gen()),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum StateEntry {
    Coin(CoinConfig),
    Message(MessageConfig),
    Contract(ContractConfig),
    ContractState(ContractState),
    ContractBalance(ContractBalance),
}

// ------------ Other stuff --------------

/// Used for primitive number types which don't implement AsRef or TryFrom<&[u8]>
pub struct HexNumber;

impl SerializeAs<BlockHeight> for HexNumber {
    fn serialize_as<S>(value: &BlockHeight, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let number: u32 = (*value).into();
        HexNumber::serialize_as(&number, serializer)
    }
}

impl<'de> DeserializeAs<'de, BlockHeight> for HexNumber {
    fn deserialize_as<D>(deserializer: D) -> Result<BlockHeight, D::Error>
    where
        D: Deserializer<'de>,
    {
        let number: u32 = HexNumber::deserialize_as(deserializer)?;
        Ok(number.into())
    }
}

impl SerializeAs<DaBlockHeight> for HexNumber {
    fn serialize_as<S>(value: &DaBlockHeight, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let number: u64 = (*value).into();
        HexNumber::serialize_as(&number, serializer)
    }
}

impl<'de> DeserializeAs<'de, DaBlockHeight> for HexNumber {
    fn deserialize_as<D>(deserializer: D) -> Result<DaBlockHeight, D::Error>
    where
        D: Deserializer<'de>,
    {
        let number: u64 = HexNumber::deserialize_as(deserializer)?;
        Ok(number.into())
    }
}

pub struct HexType;

impl<T: AsRef<[u8]>> SerializeAs<T> for HexType {
    fn serialize_as<S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde_hex::serialize(value, serializer)
    }
}

impl<'de, T, E> DeserializeAs<'de, T> for HexType
where
    for<'a> T: TryFrom<&'a [u8], Error = E>,
    E: fmt::Display,
{
    fn deserialize_as<D>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde_hex::deserialize(deserializer)
    }
}

pub mod serde_hex {
    use core::fmt;
    use std::convert::TryFrom;

    use hex::{FromHex, ToHex};
    use serde::{de::Error, Deserializer, Serializer};

    pub fn serialize<T, S>(target: T, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: ToHex,
    {
        let s = format!("0x{}", target.encode_hex::<String>());
        ser.serialize_str(&s)
    }

    pub fn deserialize<'de, T, E, D>(des: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        for<'a> T: TryFrom<&'a [u8], Error = E>,
        E: fmt::Display,
    {
        let raw_string: String = serde::Deserialize::deserialize(des)?;
        let stripped_prefix = raw_string.trim_start_matches("0x");
        let bytes: Vec<u8> = FromHex::from_hex(stripped_prefix).map_err(D::Error::custom)?;
        let result = T::try_from(bytes.as_slice()).map_err(D::Error::custom)?;
        Ok(result)
    }
}

macro_rules! impl_hex_number {
    ($i:ident) => {
        impl SerializeAs<$i> for HexNumber {
            fn serialize_as<S>(value: &$i, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let bytes = value.to_be_bytes();
                serde_hex::serialize(bytes, serializer)
            }
        }

        impl<'de> DeserializeAs<'de, $i> for HexNumber {
            fn deserialize_as<D>(deserializer: D) -> Result<$i, D::Error>
            where
                D: Deserializer<'de>,
            {
                const SIZE: usize = core::mem::size_of::<$i>();
                let mut bytes: Vec<u8> = serde_hex::deserialize(deserializer)?;
                match bytes.len() {
                    len if len > SIZE => {
                        return Err(D::Error::custom(format!(
                            "value cant exceed {WORD_SIZE} bytes"
                        )))
                    }
                    len if len < SIZE => {
                        // pad if length < word size
                        bytes = (0..SIZE - len)
                            .map(|_| 0u8)
                            .chain(bytes.into_iter())
                            .collect();
                    }
                    _ => {}
                }
                // We've already verified the bytes.len == WORD_SIZE, force the conversion here.
                Ok($i::from_be_bytes(
                    bytes.try_into().expect("byte lengths checked"),
                ))
            }
        }
    };
}

impl_hex_number!(u8);
impl_hex_number!(u16);
impl_hex_number!(u32);
impl_hex_number!(u64);

#[cfg(test)]
mod tests {
    use parquet::file::writer::SerializedFileWriter;

    use super::*;

    #[test]
    fn heyhay() {
        let schema = Arc::new(ContractConfig::schema());
        let mut buf = vec![];
        let mut writer = SerializedFileWriter::new(&mut buf, schema, Default::default()).unwrap();
        let cc = ContractConfig::random(&mut rand::thread_rng());
        cc.write(&mut writer);
        writer.close().unwrap();

        eprintln!("{}", buf.len());
    }
}
