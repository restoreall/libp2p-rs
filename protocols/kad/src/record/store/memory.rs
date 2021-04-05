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

use super::*;

// use std::borrow::Cow;
use std::collections::HashMap;

use std::io::Result;
use libp2prs_core::datastore::DataStore;

/// In-memory implementation of a `RecordStore`.
pub struct MemoryStore {
    // /// The identity of the peer owning the store.
    // local_key: kbucket::Key<PeerId>,
    // /// The configuration of the store.
    // config: MemoryStoreConfig,
    // // /// DB
    // // db: HashMap<String, dyn DataStore>,
    // /// The stored (regular) records.
    // records: HashMap<int, int>,
    // /// The stored provider records.
    // provider: MemoryProviderDataStore,
    // /// The set of all provider records for the node identified by `local_key`.
    // ///
    // /// Must be kept in sync with `providers`.
    // provided: MemoryProviderDataStore,
}

#[derive(Default, Clone)]
pub struct MemoryStorage(HashMap<Vec<u8>, Vec<u8>>);

type MemoryIter = Box<dyn Iterator<Item=(Vec<u8>, Vec<u8>)>>;

impl MemoryStore{

}

impl DataStore for MemoryStorage {
    type Iter = MemoryIter;

    fn contains(&self, key: &[u8]) -> Result<bool> {
        Ok(self.0.contains_key(key))
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.0.get(key) {
            return Ok(Some(value.clone()));
        }
        Ok(None)
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn list(&self) -> Self::Iter {
        let map = self.0.iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<HashMap<_, _>>();

        Box::new(map.into_iter())
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.0.insert(Vec::from(key), Vec::from(value));
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> Result<()> {
        match self.0.remove(key) {
            Some(v) => {
                drop(v);
                Ok(())
            }
            None => {
                Ok(())
            }
        }
    }

    fn sync(&self) -> Result<()> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2prs_core::multihash::{Code, Multihash};
    use quickcheck::*;
    use rand::Rng;

    fn random_multihash() -> Multihash {
        Multihash::wrap(Code::Sha2_256.into(), &rand::thread_rng().gen::<[u8; 32]>()).unwrap()
    }

    fn distance(r: &ProviderRecord) -> kbucket::Distance {
        kbucket::Key::new(r.key.clone()).distance(&kbucket::Key::from(r.provider))
    }

    #[test]
    fn put_get_remove_record() {
        fn prop(r: Record) {
            let mut store = MemoryStore::new(PeerId::random());
            assert!(store.put(r.clone()).is_ok());
            assert_eq!(Some(Cow::Borrowed(&r)), store.get(&r.key));
            store.remove(&r.key);
            assert!(store.get(&r.key).is_none());
        }
        quickcheck(prop as fn(_))
    }

    #[test]
    fn add_get_remove_provider() {
        fn prop(r: ProviderRecord) {
            let mut store = MemoryStore::new(PeerId::random());
            assert!(store.add_provider(r.clone()).is_ok());
            assert!(store.providers(&r.key).contains(&r));
            store.remove_provider(&r.key, &r.provider);
            assert!(!store.providers(&r.key).contains(&r));
        }
        quickcheck(prop as fn(_))
    }

    #[test]
    fn providers_ordered_by_distance_to_key() {
        fn prop(providers: Vec<kbucket::Key<PeerId>>) -> bool {
            let mut store = MemoryStore::new(PeerId::random());
            let key = Key::from(random_multihash());

            let mut records = providers
                .into_iter()
                .map(|p| ProviderRecord::new(key.clone(), p.into_preimage(), None))
                .collect::<Vec<_>>();

            for r in &records {
                assert!(store.add_provider(r.clone()).is_ok());
            }

            records.sort_by(|r1, r2| distance(r1).cmp(&distance(r2)));
            records.truncate(store.config.max_providers_per_key);

            records == store.providers(&key).to_vec()
        }

        quickcheck(prop as fn(_) -> _)
    }

    #[test]
    fn provided() {
        let id = PeerId::random();
        let mut store = MemoryStore::new(id);
        let key = random_multihash();
        let rec = ProviderRecord::new(key, id, None);
        assert!(store.add_provider(rec.clone()).is_ok());
        assert_eq!(vec![Cow::Borrowed(&rec)], store.provided().collect::<Vec<_>>());
        store.remove_provider(&rec.key, &id);
        assert_eq!(store.provided().count(), 0);
    }

    #[test]
    fn update_provider() {
        let mut store = MemoryStore::new(PeerId::random());
        let key = random_multihash();
        let prv = PeerId::random();
        let mut rec = ProviderRecord::new(key, prv, None);
        assert!(store.add_provider(rec.clone()).is_ok());
        assert_eq!(vec![rec.clone()], store.providers(&rec.key).to_vec());
        rec.expires = Some(Instant::now());
        assert!(store.add_provider(rec.clone()).is_ok());
        assert_eq!(vec![rec.clone()], store.providers(&rec.key).to_vec());
    }

    #[test]
    fn max_provided_keys() {
        let mut store = MemoryStore::new(PeerId::random());
        for _ in 0..store.config.max_provided_keys {
            let key = random_multihash();
            let prv = PeerId::random();
            let rec = ProviderRecord::new(key, prv, None);
            let _ = store.add_provider(rec);
        }
        let key = random_multihash();
        let prv = PeerId::random();
        let rec = ProviderRecord::new(key, prv, None);
        match store.add_provider(rec) {
            Err(KadError::MaxProvidedKeys) => {}
            _ => panic!("Unexpected result"),
        }
    }

    #[test]
    fn test_provider_iterator() {
        let mut store = MemoryStore::new(PeerId::random());
        let key = random_multihash();
        for _ in 0..10 {
            let prv = PeerId::random();
            let rec = ProviderRecord::new(key, prv, None);
            let _ = store.add_provider(rec);
        }

        let r = store.providers(&Key::from(key));
        assert_eq!(r.len(), 10);

        let v = store.all_providers().count();
        assert_eq!(v, 10);
    }
}
