use alloc::boxed::Box;
use alloc::collections::btree_map::BTreeMap;
use core::any::{Any, TypeId};

/// A heterogeneous container keyed by type, like `http::Extensions`: each type
/// `T` has at most one entry, inserted and retrieved by its `TypeId`.
#[derive(Default)]
pub struct Extensions(BTreeMap<TypeId, Box<dyn Any + Send + Sync>>);

impl Extensions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a value, returning the previous value of the same type if any.
    pub fn insert<T: Send + Sync + 'static>(&mut self, val: T) -> Option<T> {
        self.0
            .insert(TypeId::of::<T>(), Box::new(val))
            .and_then(|prev| prev.downcast::<T>().ok())
            .map(|boxed| *boxed)
    }

    /// Get a shared reference to the stored value of type `T`.
    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.0
            .get(&TypeId::of::<T>())
            .and_then(|b| b.downcast_ref::<T>())
    }

    /// Get a mutable reference to the stored value of type `T`.
    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.0
            .get_mut(&TypeId::of::<T>())
            .and_then(|b| b.downcast_mut::<T>())
    }

    /// Remove and return the stored value of type `T`.
    pub fn remove<T: 'static>(&mut self) -> Option<T> {
        self.0
            .remove(&TypeId::of::<T>())
            .and_then(|b| b.downcast::<T>().ok())
            .map(|boxed| *boxed)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct UserId(u64);
    #[derive(Debug, PartialEq)]
    struct Deadline(u64);

    #[test]
    fn insert_get_by_type() {
        let mut ext = Extensions::new();
        assert!(ext.is_empty());

        ext.insert(UserId(7));
        ext.insert(Deadline(1234));

        // Each type is retrieved independently, no key mismatch possible.
        assert_eq!(ext.get::<UserId>(), Some(&UserId(7)));
        assert_eq!(ext.get::<Deadline>(), Some(&Deadline(1234)));
        assert_eq!(ext.len(), 2);
    }

    #[test]
    fn insert_replaces_same_type() {
        let mut ext = Extensions::new();
        assert_eq!(ext.insert(UserId(1)), None);
        assert_eq!(ext.insert(UserId(2)), Some(UserId(1)));
        assert_eq!(ext.get::<UserId>(), Some(&UserId(2)));
    }

    #[test]
    fn get_mut_and_remove() {
        let mut ext = Extensions::new();
        ext.insert(UserId(1));
        if let Some(u) = ext.get_mut::<UserId>() {
            u.0 = 99;
        }
        assert_eq!(ext.remove::<UserId>(), Some(UserId(99)));
        assert_eq!(ext.get::<UserId>(), None);
        assert!(ext.is_empty());
    }
}
