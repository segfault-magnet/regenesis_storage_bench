use std::iter::repeat_with;

use fuel_types::{AssetId, Bytes32};
use rand::Rng;

use crate::serde_types::{
    CoinConfig, ContractBalance, ContractConfig, ContractState, MessageConfig, StateEntry,
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

pub fn payload(repeat: usize) -> Vec<StateEntry> {
    // let mut rng = rand::rngs::mock::StepRng::new(0, 1);
    let mut rng = rand::thread_rng();

    let coins = {
        let mut rng = rng.clone();
        repeat_with(move || StateEntry::Coin(CoinConfig::random(&mut rng))).take(repeat / 3)
    };
    let messages = {
        let mut rng = rng.clone();
        repeat_with(move || StateEntry::Message(MessageConfig::random(&mut rng))).take(repeat / 3)
    };

    let contracts = {
        let mut rng_clone = rng.clone();
        let contract =
            repeat_with(move || StateEntry::Contract(ContractConfig::random(&mut rng_clone)));

        let mut rng_clone = rng.clone();
        let state = repeat_with(move || {
            StateEntry::ContractState(ContractState {
                key: random_bytes_32(&mut rng_clone),
                value: random_bytes_32(&mut rng_clone),
            })
        });

        let balance = repeat_with(|| {
            StateEntry::ContractBalance(ContractBalance {
                asset_id: AssetId::new(*random_bytes_32(&mut rng)),
                amount: rng.gen(),
            })
        });

        contract
            .take(repeat / 3)
            .chain(state.take(10_000))
            .chain(balance.take(100))
    };

    coins.chain(messages).chain(contracts).collect()
}
