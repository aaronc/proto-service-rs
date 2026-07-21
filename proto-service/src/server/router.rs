use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::sync::Arc;

use crate::server::{DynService, Service};

/// A dispatch table mapping fully-qualified service names to type-erased services.
/// Transport-agnostic: build once, wrap in `Arc`, and share across any number of
/// transports.
#[derive(Default)]
pub struct ServiceRouter {
    services: BTreeMap<String, Arc<dyn DynService>>,
}

impl ServiceRouter {
    /// Registers a service under its `SERVICE_NAME`; the last registration for a name wins.
    pub fn register<T: Service + 'static>(&mut self, service: T) {
        self.services
            .insert(T::SERVICE_NAME.into(), Arc::new(service));
    }

    /// Looks up a service by fully-qualified name.
    pub fn resolve(&self, service_name: &str) -> Option<Arc<dyn DynService>> {
        self.services.get(service_name).cloned()
    }
}
