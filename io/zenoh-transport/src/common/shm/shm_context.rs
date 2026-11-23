//
// Copyright (c) 2025 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

use std::sync::Arc;

use zenoh_result::ZResult;
use zenoh_shm::{api::client_storage::GLOBAL_CLIENT_STORAGE, reader::ShmReader};

use crate::{
    common::shm::interop::{
        LazyShmProvider, MulticastTransportShmConfig, ShmOptimizationPolicy, TransportShmConfig,
    },
    unicast::establishment::ext::shm::auth::AuthUnicast,
};

#[derive(Clone)]
pub(crate) struct MulticastTransportShmContext {
    pub(crate) shm_reader: ShmReader,
    pub(crate) shm_provider: Option<Arc<LazyShmProvider>>,
    pub(crate) shm_config: MulticastTransportShmConfig,
    pub(crate) policy: ShmOptimizationPolicy,
}

impl MulticastTransportShmContext {
    pub(crate) fn new(
        shm_reader: ShmReader,
        shm_provider: Option<Arc<LazyShmProvider>>,
        policy: ShmOptimizationPolicy,
    ) -> Self {
        Self {
            shm_reader,
            shm_provider,
            shm_config: MulticastTransportShmConfig,
            policy,
        }
    }
}

#[derive(Clone)]
pub(crate) struct UnicastTransportShmContext {
    pub(crate) shm_reader: ShmReader,
    pub(crate) shm_provider: Option<Arc<LazyShmProvider>>,
    pub(crate) shm_config: TransportShmConfig,
    pub(crate) policy: ShmOptimizationPolicy,
}

impl UnicastTransportShmContext {
    pub(crate) fn new(
        shm_reader: ShmReader,
        shm_provider: Option<Arc<LazyShmProvider>>,
        shm_config: TransportShmConfig,
        policy: ShmOptimizationPolicy,
    ) -> Self {
        Self {
            shm_reader,
            shm_provider,
            shm_config,
            policy,
        }
    }
}

pub struct ShmContext {
    pub(crate) shm_reader: ShmReader,
    pub(crate) shm_provider: Option<Arc<LazyShmProvider>>,
    pub(crate) policy: ShmOptimizationPolicy,
    pub(crate) auth: AuthUnicast,
}

impl std::fmt::Debug for ShmContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShmContext")
            .field("shm_reader", &self.shm_reader)
            .field("shm_provider", &self.shm_provider)
            .field("auth", &"..")
            .finish()
    }
}

impl ShmContext {
    pub fn new(
        cfg: zenoh_config::ShmConf,
        external_reader: Option<ShmReader>,
    ) -> ZResult<Option<Self>> {
        if !*cfg.enabled() {
            return Ok(None);
        }

        // THE MANDATORY SEGMENTS FIRST (BER-2171). The auth segment is required
        // -- its failure is a `?` that refuses the session -- while the pool
        // below is best-effort and degrades to the network path, so a shortfall
        // must fall on the pool rather than refuse the session.
        let shm_reader = external_reader
            .unwrap_or_else(|| ShmReader::new((*GLOBAL_CLIENT_STORAGE.read()).clone()));

        let auth = AuthUnicast::new(&shm_reader.supported_protocols())?;

        let shm_provider = if *cfg.transport_optimization.enabled() {
            let provider = Arc::new(LazyShmProvider::new(
                *cfg.transport_optimization.pool_size(),
                *cfg.transport_optimization.message_size_threshold(),
            ));
            // EAGER MEANS THE PROVIDER TOO, not only the globals.
            //
            // `ShmInitMode::Init` was honoured one layer up, by
            // `zenoh_shm::init::init()`, which initialises the subsystem
            // globals -- client storage, metadata, cleanup and both watchdogs.
            // The TRANSPORT's provider was not covered, so the segment was
            // still allocated on the first SHM interaction, mid-session, which
            // is precisely what `init` promises not to do: "guarantees no
            // latency impact when first SHM buffer is processed".
            //
            // Doing it here rather than at the `init()` call site because this
            // is where the provider is constructed and where `ShmConf` is in
            // hand; the alternative would be plumbing the provider back up to
            // the runtime purely to initialise it.
            if matches!(cfg.mode(), zenoh_config::ShmInitMode::Init) {
                provider.init_eagerly();
            }
            Some(provider)
        } else {
            None
        };

        let messages = cfg.transport_optimization.messages();
        let policy = ShmOptimizationPolicy {
            put: messages.contains(&zenoh_config::DataMessage::Put),
            query: messages.contains(&zenoh_config::DataMessage::Query),
            reply: messages.contains(&zenoh_config::DataMessage::Reply),
        };

        Ok(Some(Self {
            shm_provider,
            policy,
            shm_reader,
            auth,
        }))
    }

    pub fn shm_provider(&self) -> &Option<Arc<LazyShmProvider>> {
        &self.shm_provider
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **IN `init` MODE THE CONTEXT HAS A POOL ID TO ADVERTISE** (BER-2169,
    /// BER-2173).
    ///
    /// The layer between the config flag and the handshake. If this is `None`
    /// the handshake advertises nothing and the pre-mount silently never
    /// happens, which is indistinguishable from success at every level above.
    #[test]
    fn init_mode_yields_a_pool_id_to_advertise() {
        let mut cfg = zenoh_config::ShmConf::default();
        cfg.set_enabled(true).unwrap();
        cfg.set_mode(zenoh_config::ShmInitMode::Init).unwrap();
        // Small: the test needs a pool id to advertise, not the default 16 MiB.
        cfg.transport_optimization
            .set_pool_size(std::num::NonZeroUsize::new(64 * 1024).unwrap())
            .unwrap();

        let context = ShmContext::new(cfg, None)
            .expect("context creation failed")
            .expect("shm was enabled, so a context must exist");

        let provider = context
            .shm_provider
            .as_ref()
            .expect("transport optimisation is on by default, so a provider must exist");

        assert!(
            provider.data_segment_id().is_some(),
            "eager mode must leave a pool id to advertise; None means the handshake offers \
             nothing and the peer silently mounts on first use instead"
        );
    }

    /// The control: in `lazy` mode there is deliberately nothing to advertise.
    #[test]
    fn lazy_mode_has_no_pool_id_to_advertise() {
        let mut cfg = zenoh_config::ShmConf::default();
        cfg.set_enabled(true).unwrap();
        cfg.set_mode(zenoh_config::ShmInitMode::Lazy).unwrap();

        let context = ShmContext::new(cfg, None).unwrap().unwrap();
        let provider = context.shm_provider.as_ref().unwrap();

        assert!(
            provider.data_segment_id().is_none(),
            "lazy mode must not have built a pool yet"
        );
    }
}
