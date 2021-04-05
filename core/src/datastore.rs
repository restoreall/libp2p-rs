use std::io::Result;

pub trait DataStore {
    type Iter: Iterator<Item=(Vec<u8>, Vec<u8>)>;

    fn contains(&self, key: &[u8]) -> Result<bool>;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn len(&self) -> usize;
    fn list(&self) -> Self::Iter;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn remove(&mut self, key: &[u8]) -> Result<()>;

    fn sync(&self) -> Result<()>;
}