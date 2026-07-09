use alloc::borrow::Cow;
use alloc::collections::btree_map::{self, BTreeMap};
use alloc::string::String;
use alloc::vec::Vec;

/// A multimap of string keys to string values, analogous to HTTP headers / gRPC
/// metadata. Keys are case-insensitive ASCII, stored lowercased.
/// Binary (-bin) metadata should be base64 and it is the responsibility of the implementor
/// to encode/decode.
#[derive(Clone, Debug, Default)]
pub struct MetadataMap {
    data: BTreeMap<String, Vec<String>>,
}

/// Lowercase an ASCII key for lookup, borrowing when it's already lowercase.
fn normalize(key: &str) -> Cow<'_, str> {
    if key.bytes().any(|b| b.is_ascii_uppercase()) {
        Cow::Owned(key.to_ascii_lowercase())
    } else {
        Cow::Borrowed(key)
    }
}

/// Lowercase an owned key in place (no extra allocation).
fn normalize_owned(key: impl Into<String>) -> String {
    let mut key = key.into();
    key.make_ascii_lowercase();
    key
}

impl MetadataMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a value, replacing all existing values for this key.
    /// Returns the previous values if any.
    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Option<Vec<String>> {
        self.data
            .insert(normalize_owned(key), alloc::vec![value.into()])
    }

    /// Append a value to the given key without removing existing values.
    pub fn append(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.data
            .entry(normalize_owned(key))
            .or_default()
            .push(value.into());
    }

    /// Get the first value for a key.
    pub fn get_first(&self, key: &str) -> Option<&str> {
        self.data
            .get(normalize(key).as_ref())
            .and_then(|v| v.first())
            .map(|v| v.as_str())
    }

    /// Get all values for a key.
    pub fn get_all(&self, key: &str) -> &[String] {
        self.data
            .get(normalize(key).as_ref())
            .map(|v| v.as_slice())
            .unwrap_or_default()
    }

    /// Remove all values for a key, returning them.
    pub fn remove(&mut self, key: &str) -> Option<Vec<String>> {
        self.data.remove(normalize(key).as_ref())
    }

    /// Returns true if the map contains no entries.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the number of keys.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Iterate over all key-values pairs. Keys are lowercased.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &[String])> {
        self.data.iter().map(|(k, v)| (k.as_str(), v.as_slice()))
    }

    /// Iterate over all individual key-value pairs (flattened). Keys are lowercased.
    pub fn iter_flat(&self) -> impl Iterator<Item = (&str, &str)> {
        self.data
            .iter()
            .flat_map(|(k, vs)| vs.iter().map(move |v| (k.as_str(), v.as_str())))
    }

    pub fn keys(&self) -> btree_map::Keys<'_, String, Vec<String>> {
        self.data.keys()
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get() {
        let mut m = MetadataMap::new();
        m.insert("content-type", "application/grpc");
        assert_eq!(m.get_first("content-type"), Some("application/grpc"));
        assert_eq!(m.get_first("missing"), None);
    }

    #[test]
    fn keys_are_case_insensitive() {
        let mut m = MetadataMap::new();
        m.insert("Content-Type", "application/grpc");
        // Lookup with any casing hits the same entry.
        assert_eq!(m.get_first("content-type"), Some("application/grpc"));
        assert_eq!(m.get_first("CONTENT-TYPE"), Some("application/grpc"));
        // Appending with different casing extends the same key rather than forking it.
        m.append("CONTENT-type", "extra");
        assert_eq!(m.get_all("content-type").len(), 2);
        assert_eq!(m.len(), 1);
        // Stored key is normalized to lowercase.
        assert_eq!(
            m.keys().map(|k| k.as_str()).collect::<Vec<_>>(),
            alloc::vec!["content-type"]
        );
    }

    #[test]
    fn append_and_get_all() {
        let mut m = MetadataMap::new();
        m.append("x-custom", "a");
        m.append("x-custom", "b");
        assert_eq!(m.get_first("x-custom"), Some("a"));
        assert_eq!(m.get_all("x-custom").len(), 2);
    }

    #[test]
    fn insert_replaces() {
        let mut m = MetadataMap::new();
        m.append("key", "first");
        m.append("key", "second");
        let old = m.insert("key", "replaced");
        assert_eq!(old.unwrap().len(), 2);
        assert_eq!(m.get_all("key").len(), 1);
    }

    #[test]
    fn iter_flat_counts_all_values() {
        let mut m = MetadataMap::new();
        m.append("a", "1");
        m.append("a", "2");
        m.append("b", "3");
        let flat: Vec<_> = m.iter_flat().collect();
        assert_eq!(flat.len(), 3);
    }
}
