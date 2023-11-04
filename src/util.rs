use std::{
    io::{BufReader, Cursor},
    iter::repeat_with,
};

use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use fuel_types::{AssetId, Bytes32};
use rand::Rng;

use crate::serde_types::{
    CoinConfig, ContractBalance, ContractConfig, ContractState, MessageConfig,
};

pub fn random_bytes_32(rng: &mut impl Rng) -> Bytes32 {
    Bytes32::from(rng.gen::<[u8; 32]>())
}

#[derive(Default, Debug)]
pub struct CountingSink {
    pub written_bytes: usize,
}

impl std::io::Write for CountingSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = buf.len();
        self.written_bytes += len;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct Payload {
    pub coins: Vec<CoinConfig>,
    pub messages: Vec<MessageConfig>,
    pub contracts: Vec<ContractConfig>,
    pub contract_state: Vec<ContractState>,
    pub contract_balance: Vec<ContractBalance>,
}

impl Payload {
    pub fn num_entries(&self) -> usize {
        self.coins.len() + self.messages.len() + self.contracts.len()
    }
}

pub struct Data<T> {
    pub coins: T,
    pub messages: T,
    pub contracts: T,
    pub contract_state: T,
    pub contract_balance: T,
}

impl Data<&mut Vec<u8>> {
    #[must_use]
    pub fn len(&self) -> usize {
        self.coins.len()
            + self.messages.len()
            + self.contracts.len()
            + self.contract_state.len()
            + self.contract_balance.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn wrap_in_buffered_decompressor(&self) -> Data<BufReader<ZlibDecoder<&[u8]>>> {
        Data {
            coins: BufReader::new(ZlibDecoder::new(self.coins.as_slice())),
            messages: BufReader::new(ZlibDecoder::new(self.messages.as_slice())),
            contracts: BufReader::new(ZlibDecoder::new(self.contracts.as_slice())),
            contract_state: BufReader::new(ZlibDecoder::new(self.contract_state.as_slice())),
            contract_balance: BufReader::new(ZlibDecoder::new(self.contract_balance.as_slice())),
        }
    }
}
impl Data<Vec<u8>> {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            coins: Vec::with_capacity(cap),
            messages: Vec::with_capacity(cap),
            contracts: Vec::with_capacity(cap),
            contract_state: Vec::with_capacity(cap),
            contract_balance: Vec::with_capacity(cap),
        }
    }
    pub fn clear(&mut self) {
        self.coins.clear();
        self.messages.clear();
        self.contracts.clear();
        self.contract_state.clear();
        self.contract_balance.clear();
    }

    pub fn len(&self) -> usize {
        self.coins.len()
            + self.messages.len()
            + self.contracts.len()
            + self.contract_state.len()
            + self.contract_balance.len()
    }

    pub fn as_ref(&self) -> Data<&[u8]> {
        Data {
            coins: self.coins.as_slice(),
            messages: self.messages.as_slice(),
            contracts: self.contracts.as_slice(),
            contract_state: self.contract_state.as_slice(),
            contract_balance: self.contract_balance.as_slice(),
        }
    }

    pub fn wrap_in_compressor(&mut self, level: Compression) -> Data<ZlibEncoder<&mut Vec<u8>>> {
        Data {
            coins: ZlibEncoder::new(&mut self.coins, level),
            messages: ZlibEncoder::new(&mut self.messages, level),
            contracts: ZlibEncoder::new(&mut self.contracts, level),
            contract_state: ZlibEncoder::new(&mut self.contract_state, level),
            contract_balance: ZlibEncoder::new(&mut self.contract_balance, level),
        }
    }

    pub fn wrap_in_cursor(self) -> Data<Cursor<Vec<u8>>> {
        Data {
            coins: Cursor::new(self.coins),
            messages: Cursor::new(self.messages),
            contracts: Cursor::new(self.contracts),
            contract_state: Cursor::new(self.contract_state),
            contract_balance: Cursor::new(self.contract_balance),
        }
    }
}

impl<'a> Data<ZlibEncoder<&'a mut Vec<u8>>> {
    pub fn finish(self) -> std::io::Result<Data<&'a mut Vec<u8>>> {
        Ok(Data {
            coins: self.coins.finish()?,
            messages: self.messages.finish()?,
            contracts: self.contracts.finish()?,
            contract_state: self.contract_state.finish()?,
            contract_balance: self.contract_balance.finish()?,
        })
    }
}
impl Data<&mut Vec<u8>> {}

pub fn payload(repeat: usize) -> Payload {
    let mut rng = rand::rngs::mock::StepRng::new(0, 1);
    // let mut rng = rand::thread_rng();

    let coins = {
        let mut rng = rng.clone();
        repeat_with(move || CoinConfig::random(&mut rng))
            .take(repeat / 3)
            .collect()
    };
    let messages = {
        let mut rng = rng.clone();
        repeat_with(move || MessageConfig::random(&mut rng))
            .take(repeat / 3)
            .collect()
    };

    let contracts = {
        let mut rng_clone = rng.clone();
        repeat_with(move || ContractConfig::random(&mut rng_clone))
            .take(repeat / 3)
            .collect()
    };

    let contract_state = {
        let mut rng_clone = rng.clone();
        // TODO: this number needs to be fixed to be per contract
        repeat_with(move || ContractState {
            key: random_bytes_32(&mut rng_clone),
            value: random_bytes_32(&mut rng_clone),
        })
        .take(10_000)
        .collect()
    };
    let contract_balance = {
        // TODO: this number needs to be fixed to be per contract
        repeat_with(|| ContractBalance {
            asset_id: AssetId::new(*random_bytes_32(&mut rng)),
            amount: rng.gen(),
        })
        .take(100)
        .collect()
    };

    Payload {
        coins,
        messages,
        contracts,
        contract_state,
        contract_balance,
    }
}
