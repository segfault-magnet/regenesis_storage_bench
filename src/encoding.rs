mod bincode_codec;
mod bson_codec;
mod json_codec;
mod parquet_codec;

pub use bincode_codec::*;
pub use bson_codec::*;
pub use json_codec::*;
pub use parquet_codec::*;

use crate::{
    serde_types::{CoinConfig, ContractBalance, ContractConfig, ContractState, MessageConfig},
    util::{Data, Payload},
};

pub trait PayloadCodec<R, W> {
    fn encode(&self, payload: Payload, writers: &mut Data<W>);
    fn decode(&self, readers: Data<R>);
}

impl<
        R,
        W,
        T: Encode<CoinConfig, W>
            + Decode<CoinConfig, R>
            + Encode<ContractConfig, W>
            + Decode<ContractConfig, R>
            + Encode<MessageConfig, W>
            + Decode<MessageConfig, R>
            + Encode<ContractState, W>
            + Decode<ContractState, R>
            + Encode<ContractBalance, W>
            + Decode<ContractBalance, R>,
    > PayloadCodec<R, W> for T
{
    fn encode(&self, payload: Payload, writers: &mut Data<W>) {
        self.encode_subset(payload.coins, &mut writers.coins);
        self.encode_subset(payload.messages, &mut writers.messages);
        self.encode_subset(payload.contracts, &mut writers.contracts);
        self.encode_subset(payload.contract_state, &mut writers.contract_state);
        self.encode_subset(payload.contract_balance, &mut writers.contract_balance);
    }
    fn decode(&self, readers: Data<R>) {
        Decode::<CoinConfig, _>::decode_subset(self, readers.coins);
        Decode::<MessageConfig, _>::decode_subset(self, readers.messages);
        Decode::<ContractConfig, _>::decode_subset(self, readers.contracts);
        Decode::<ContractState, _>::decode_subset(self, readers.contract_state);
        Decode::<ContractBalance, _>::decode_subset(self, readers.contract_balance);
    }
}

trait Encode<T, W> {
    fn encode_subset(&self, data: Vec<T>, writer: &mut W);
}

trait Decode<T, R> {
    fn decode_subset(&self, reader: R);
}
