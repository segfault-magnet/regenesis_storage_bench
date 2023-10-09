use core::fmt;
use std::convert::TryFrom;

use fuel_core_types::{blockchain::primitives::DaBlockHeight, fuel_types::bytes::WORD_SIZE};
use fuel_types::{Address, AssetId, BlockHeight, Bytes32, ContractId, Nonce, Salt, Word};
use rand::Rng;
use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{serde_as, skip_serializing_none, DeserializeAs, SerializeAs};

fn random_bytes_32(rng: &mut impl Rng) -> Bytes32 {
    Bytes32::from(rng.gen::<[u8; 32]>())
}

#[skip_serializing_none]
#[serde_as]
#[derive(Serialize, Deserialize, Clone, Debug)]
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

#[skip_serializing_none]
#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct ContractConfig {
    #[serde_as(as = "HexType")]
    pub contract_id: ContractId,
    #[serde_as(as = "HexType")]
    pub code: Vec<u8>,
    #[serde_as(as = "HexType")]
    pub salt: Salt,
    #[serde_as(as = "Option<Vec<(HexType, HexType)>>")]
    #[serde(default)]
    pub state: Option<Vec<(Bytes32, Bytes32)>>,
    #[serde_as(as = "Option<Vec<(HexType, HexNumber)>>")]
    #[serde(default)]
    pub balances: Option<Vec<(AssetId, u64)>>,
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

impl ContractConfig {
    pub fn random(rng: &mut impl Rng) -> Self {
        let balances = std::iter::repeat_with(|| {
            let asset_id = AssetId::new(*random_bytes_32(rng));
            (asset_id, rng.gen())
        })
        .take(10)
        .collect::<Vec<_>>();
        ContractConfig {
            contract_id: ContractId::new(*random_bytes_32(rng)),
            code: (*random_bytes_32(rng)).to_vec(),
            salt: Salt::new(*random_bytes_32(rng)),
            state: Some(vec![(random_bytes_32(rng), random_bytes_32(rng))]),
            balances: Some(balances),
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
    Contract(ContractConfig),
    Message(MessageConfig),
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
