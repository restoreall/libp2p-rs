// Copyright 2019 Parity Technologies (UK) Ltd.
// Copyright 2020 Netwarps Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

mod memory;

use crate::{dht_proto as proto};
use prost::Message;

pub use memory::{MemoryStorage};

use super::*;
use crate::{KadError, kbucket};
use std::borrow::Cow;
use libp2prs_core::datastore::DataStore;
use std::collections::{ HashMap, HashSet};
// use std::iter;
use crate::protocol::{provider_to_vec, provider_vec_from_proto, provider_from_proto, provider_vec_to_proto, record_to_proto, record_from_proto};
// use std::iter::{Flatten, Map, FlatMap};
// use std::collections::hash_map::IntoIter;

/// The result of an operation on a `RecordStore`.
pub type Result<T> = std::result::Result<T, KadError>;

/// Trait for types implementing a record store.
///
/// There are two types of records managed by a `RecordStore`:
///
///   1. Regular (value-)records. These records store an arbitrary value
///      associated with a key which is distributed to the closest nodes
///      to the key in the Kademlia DHT as per the standard Kademlia "push-model".
///      These records are subject to re-replication and re-publication as
///      per the standard Kademlia protocol.
///
///   2. Provider records. These records associate the ID of a peer with a key
///      who can supposedly provide the associated value. These records are
///      mere "pointers" to the data which may be followed by contacting these
///      providers to obtain the value. These records are specific to the
///      libp2p Kademlia specification and realise a "pull-model" for distributed
///      content. Just like a regular record, a provider record is distributed
///      to the closest nodes to the key.
///

pub struct ProviderStore<D: DataStore> {
    local_key: kbucket::Key<PeerId>,
    provider: D,
    // key: KEY, value: ProviderRecord
    provided: D,
    max_providers_per_key: usize,
    max_provided_keys: usize,
}

// type ProviderIter<'a> = Map<Flatten<FlatMap<IntoIter<Key, Vec<ProviderRecord>>, Vec<ProviderRecord>, fn((Key, Vec<ProviderRecord>)) -> Vec<ProviderRecord>>>, fn(&'a ProviderRecord) -> Cow<'a, ProviderRecord>>;
//
// type ProvidedIter<'a> = iter::Map<hash_set::Iter<'a, ProviderRecord>, fn(&'a ProviderRecord) -> Cow<'a, ProviderRecord>>;
// type RecordsIter<'a> = iter::Map<hash_map::Values<'a, Key, Record>, fn(&'a Record) -> Cow<'a, Record>>;

impl<'a, D> ProviderStore<D>
    where
        D: DataStore
{
    pub fn new(provider: D, provided: D, peer_id: PeerId, max_providers_per_key: usize, max_provided_keys: usize) -> Self {
        ProviderStore {
            local_key: kbucket::Key::from(peer_id),
            provider,
            provided,
            max_providers_per_key,
            max_provided_keys,
        }
    }

    pub fn add_provider(&'a mut self, record: ProviderRecord) -> Result<()> {
        let provider_key = record.key.clone();
        return match self.provider.get(record.key.as_ref()) {
            // key is in store, get it out and add new record
            Ok(Some(proto)) => {
                let decode_result = proto::ProviderVector::decode(&proto[..]).unwrap();
                let mut providers = provider_vec_from_proto(decode_result)?;
                if let Some(i) = providers.iter().position(|p| p.provider == record.provider) {
                    providers[i] = record;
                } else {
                    let local_key = self.local_key.clone();
                    let key = kbucket::Key::new(record.key.clone());
                    let provider = kbucket::Key::from(record.provider);
                    if let Some(i) = providers.iter().position(|p| {
                        let pk = kbucket::Key::from(p.provider);
                        provider.distance(&key) < pk.distance(&key)
                    }) {
                        // Insert the new provider.
                        if local_key.preimage() == &record.provider {
                            let self_record = provider_to_vec(record.clone());
                            self.provided.put(provider_key.as_ref(), &*self_record)?;
                        }
                        providers.insert(i, record);
                        // Remove the excess provider, if any.
                        if providers.len() > self.max_providers_per_key {
                            if let Some(p) = providers.pop() {
                                let _ = self.provided.remove(p.key.as_ref());
                            }
                        }
                    } else if providers.len() < self.max_providers_per_key {
                        // The distance of the new provider to the key is larger than
                        // the distance of any existing provider, but there is still room.
                        if local_key.preimage() == &record.provider {
                            let self_record = provider_to_vec(record.clone());
                            if let Err(e) = self.provided.put(provider_key.as_ref(), &*self_record) {
                                return Err(KadError::from(e));
                            }
                        }
                        providers.push(record);
                    }
                }

                let proto = provider_vec_to_proto(providers);
                let mut buf: Vec<u8> = Vec::with_capacity(proto.encoded_len());
                proto.encode(&mut buf).expect("Vec<u8> provides capacity as needed");

                self.provider.put(provider_key.as_ref(), &*buf).map_err(|e| KadError::from(e))
            }
            Ok(None) => {
                if self.local_key.preimage() == &record.provider {
                    let self_record = record.clone();
                    let value = provider_to_vec(self_record);
                    self.provided.put(provider_key.as_ref(), &*value).map_err(|e| KadError::from(e))?;
                }

                let vec = vec![record];
                let proto = provider_vec_to_proto(vec);
                let mut buf: Vec<u8> = Vec::with_capacity(proto.encoded_len());
                proto.encode(&mut buf).expect("Vec<u8> provides capacity as needed");

                self.provider.put(provider_key.as_ref(), &*buf).map_err(|e| KadError::from(e))
            }
            Err(e) => {
                Err(KadError::from(e))
            }
        };
    }

    pub fn providers(&'a self, key: &Key) -> Vec<ProviderRecord> {
        if let Ok(Some(proto)) = self.provider.get(key.as_ref()) {
            let decode_result = proto::ProviderVector::decode(&proto[..]).unwrap();
            match provider_vec_from_proto(decode_result) {
                Ok(value) => {
                    return value;
                }
                _ => {}
            }
        }

        vec![]
    }

    pub fn all_providers(&'a self) -> HashMap<Key, Vec<ProviderRecord>> {
        self.provider.list()
            .map(|(key, value)| {
                let decode_result = proto::ProviderVector::decode(&value[..]).unwrap();
                (Key::from(key), provider_vec_from_proto(decode_result).unwrap())
            })
            .collect::<HashMap<_, _>>()
    }

    pub fn provided(&'a self) -> HashSet<ProviderRecord> {
        self.provided.list()
            .map(|(_, value)| {
                let decode_result = proto::Provider::decode(&value[..]).unwrap();
                provider_from_proto(decode_result).unwrap()
            })
            .collect::<HashSet<_>>()
    }

    pub fn remove_provider(&'a mut self, key: &Key, provider: &PeerId) {
        match self.provider.get(key.as_ref()) {
            Ok(Some(proto_value)) => {
                let decode = proto::ProviderVector::decode(&proto_value[..]).unwrap();
                let providers = provider_vec_from_proto(decode).unwrap();
                let providers = providers.iter()
                    .filter(|p| p.provider != provider.clone())
                    .map(|p| p.clone())
                    .collect::<Vec<_>>();

                let proto = provider_vec_to_proto(providers);
                let mut buf: Vec<u8> = Vec::with_capacity(proto.encoded_len());
                proto.encode(&mut buf).expect("Vec<u8> provides capacity as needed");
                let _ = self.provider.put(key.as_ref(), &*buf);
            }
            _ => {}
        }
    }
}

pub struct RecordStore<D: DataStore> {
    max_value_bytes: usize,
    record: D,
}

impl<'a, D> RecordStore<D>
    where
        D: DataStore
{
    pub fn new(max_value_bytes: usize, record: D) -> Self {
        RecordStore {
            max_value_bytes,
            record,
        }
    }

    /// Gets a record from the store, given its key.
    pub(crate) fn get(&'a self, k: &Key) -> Option<Cow<'_, Record>> {
        if let Ok(Some(value)) = self.record.get(k.as_ref()) {
            let decode = proto::Record::decode(&value[..]).unwrap();
            let record = record_from_proto(decode).unwrap();
            return Some(record).map(Cow::Owned);
        }

        None
    }

    pub(crate) fn put(&'a mut self, r: Record) -> Result<()> {
        if r.clone().value.len() >= self.max_value_bytes {
            return Err(KadError::ValueTooLarge);
        }

        let proto = record_to_proto(r.clone());
        let mut buf: Vec<u8> = Vec::with_capacity(proto.encoded_len());
        proto.encode(&mut buf).expect("Vec<u8> provides capacity as needed");

        self.record.put(r.key.clone().as_ref(), &*buf).map_err(|e| KadError::from(e))
    }

    pub(crate) fn remove(&'a mut self, k: &Key) {
        let _ = self.record.remove(k.as_ref());
    }

    pub(crate) fn records(&'a self) -> HashMap<Key, Record> {
        self.record.list()
            .map(|(key, value)| {
                let proto = proto::Record::decode(&value[..]).unwrap();
                let record = record_from_proto(proto).unwrap();
                let k = Key::from(key);
                (k, record)
            })
            .collect::<HashMap<_, _>>()
    }
}