use crate::serde_types::{CoinConfig, ContractConfig, MessageConfig, StateEntry};

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
    let mut rng = rand::rngs::mock::StepRng::new(0, 1);

    let coins = {
        let mut rng = rng.clone();
        std::iter::repeat_with(move || StateEntry::Coin(CoinConfig::random(&mut rng)))
            .take(repeat / 3)
    };
    let contracts = {
        let mut rng = rng.clone();
        std::iter::repeat_with(move || StateEntry::Contract(ContractConfig::random(&mut rng)))
            .take(repeat / 3)
    };
    let messages =
        std::iter::repeat_with(move || StateEntry::Message(MessageConfig::random(&mut rng)))
            .take(repeat / 3);

    coins.chain(messages).chain(contracts).collect()
}
