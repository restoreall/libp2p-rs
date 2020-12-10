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

use std::time::{Duration, Instant};
use std::num::NonZeroUsize;
use std::borrow::Borrow;
use smallvec::SmallVec;
use std::sync::{Arc};
use fnv::{FnvHashSet, FnvHashMap};

use futures::stream::FusedStream;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
    select,
};
use futures::lock::Mutex;

use async_std::task;

use libp2prs_core::{PeerId, Multiaddr};
use libp2prs_swarm::Control as SwarmControl;

use crate::protocol::{KadProtocolHandler, KadPeer, ProtocolEvent, KadRequestMsg, KadResponseMsg, KadConnectionType, KademliaProtocolConfig, KadMessenger};
use crate::control::{Control, ControlCommand};

use crate::query::{QueryConfig, QueryStats, IterativeQuery, QueryType, PeerRecord};
use crate::jobs::{AddProviderJob, PutRecordJob};
use crate::kbucket::{KBucketsTable, NodeStatus};
use crate::store::RecordStore;
use crate::{record, kbucket, Addresses, Record, KadError, ProviderRecord};
use crate::task_limit::TaskLimiter;
use async_std::task::JoinHandle;


type Result<T> = std::result::Result<T, KadError>;


/// `Kademlia` implements the libp2p Kademlia protocol.
pub struct Kademlia<TStore> {
    /// The Kademlia routing table.
    kbuckets: KBucketsTable<kbucket::Key<PeerId>, Addresses>,

    /// The k-bucket insertion strategy.
    kbucket_inserts: KademliaBucketInserts,

    /// Configuration of the wire protocol.
    protocol_config: KademliaProtocolConfig,

    /// The config for queries.
    query_config: QueryConfig,

    /// The cache of Kademlia messenger.
    messengers: Option<MessengerManager>,

    /// The currently connected peers.
    ///
    /// This is a superset of the connected peers currently in the routing table.
    connected_peers: FnvHashSet<PeerId>,

    /// The timer task handle of Provider cleanup job.
    provider_timer_handle: Option<JoinHandle<()>>,

    /// Periodic job for re-publication of provider records for keys
    /// provided by the local node.
    add_provider_job: Option<AddProviderJob>,

    /// Periodic job for (re-)replication and (re-)publishing of
    /// regular (value-)records.
    put_record_job: Option<PutRecordJob>,

    /// The interval to cleanup expired provider records.
    cleanup_interval: Duration,

    /// The TTL of regular (value-)records.
    record_ttl: Option<Duration>,

    /// The TTL of provider records.
    provider_record_ttl: Option<Duration>,

    /// How long to keep connections alive when they're idle.
    connection_idle_timeout: Duration,

    // /// Queued events to return when the behaviour is being polled.
    // queued_events: VecDeque<NetworkBehaviourAction<KademliaHandlerIn<QueryId>, KademliaEvent>>,

    /// The currently known addresses of the local node.
    local_addrs: FnvHashSet<Multiaddr>,

    /// The record storage.
    store: TStore,

    // Used to communicate with Swarm.
    swarm: Option<SwarmControl>,

    // New peer is connected or peer is dead.
    // peer_tx: mpsc::UnboundedSender<PeerEvent>,
    // peer_rx: mpsc::UnboundedReceiver<PeerEvent>,

    /// Used to handle the incoming Kad events.
    event_tx: mpsc::UnboundedSender<ProtocolEvent>,
    event_rx: mpsc::UnboundedReceiver<ProtocolEvent>,

    /// Used to control the Kademlia.
    /// control_tx becomes the Control and control_rx is monitored by
    /// the Kademlia main loop.
    control_tx: mpsc::UnboundedSender<ControlCommand>,
    control_rx: mpsc::UnboundedReceiver<ControlCommand>,
}


/// The configurable strategies for the insertion of peers
/// and their addresses into the k-buckets of the Kademlia
/// routing table.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum KademliaBucketInserts {
    /// Whenever a connection to a peer is established as a
    /// result of a dialing attempt and that peer is not yet
    /// in the routing table, it is inserted as long as there
    /// is a free slot in the corresponding k-bucket. If the
    /// k-bucket is full but still has a free pending slot,
    /// it may be inserted into the routing table at a later time if an unresponsive
    /// disconnected peer is evicted from the bucket.
    OnConnected,
    /// New peers and addresses are only added to the routing table via
    /// explicit calls to [`Kademlia::add_address`].
    ///
    /// > **Note**: Even though peers can only get into the
    /// > routing table as a result of [`Kademlia::add_address`],
    /// > routing table entries are still updated as peers
    /// > connect and disconnect (i.e. the order of the entries
    /// > as well as the network addresses).
    Manual,
}

/// The configuration for the `Kademlia` behaviour.
///
/// The configuration is consumed by [`Kademlia::new`].
#[derive(Debug, Clone)]
pub struct KademliaConfig {
    kbucket_pending_timeout: Duration,
    query_config: QueryConfig,
    protocol_config: KademliaProtocolConfig,
    cleanup_interval: Duration,
    record_ttl: Option<Duration>,
    record_replication_interval: Option<Duration>,
    record_publication_interval: Option<Duration>,
    provider_record_ttl: Option<Duration>,
    provider_publication_interval: Option<Duration>,
    connection_idle_timeout: Duration,
    kbucket_inserts: KademliaBucketInserts,
}

impl Default for KademliaConfig {
    fn default() -> Self {
        KademliaConfig {
            kbucket_pending_timeout: Duration::from_secs(60),
            query_config: QueryConfig::default(),
            protocol_config: Default::default(),
            cleanup_interval: Duration::from_secs(24 * 60 * 60),
            record_ttl: Some(Duration::from_secs(36 * 60 * 60)),
            record_replication_interval: Some(Duration::from_secs(60 * 60)),
            record_publication_interval: Some(Duration::from_secs(24 * 60 * 60)),
            provider_publication_interval: Some(Duration::from_secs(12 * 60 * 60)),
            provider_record_ttl: Some(Duration::from_secs(24 * 60 * 60)),
            connection_idle_timeout: Duration::from_secs(10),
            kbucket_inserts: KademliaBucketInserts::OnConnected,
        }
    }
}

impl KademliaConfig {
    /// Sets a custom protocol name.
    ///
    /// Kademlia nodes only communicate with other nodes using the same protocol
    /// name. Using a custom name therefore allows to segregate the DHT from
    /// others, if that is desired.
    pub fn set_protocol_name(&mut self, name: &'static [u8]) -> &mut Self {
        self.protocol_config.set_protocol_name(name);
        self
    }

    /// Sets the timeout for a single query.
    ///
    /// > **Note**: A single query usually comprises at least as many requests
    /// > as the replication factor, i.e. this is not a request timeout.
    ///
    /// The default is 60 seconds.
    pub fn set_query_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.query_config.timeout = timeout;
        self
    }

    /// Sets the replication factor to use.
    ///
    /// The replication factor determines to how many closest peers
    /// a record is replicated. The default is [`K_VALUE`].
    pub fn set_replication_factor(&mut self, replication_factor: NonZeroUsize) -> &mut Self {
        self.query_config.replication_factor = replication_factor;
        self
    }

    /// Sets the allowed level of parallelism for iterative queries.
    ///
    /// The `α` parameter in the Kademlia paper. The maximum number of peers
    /// that an iterative query is allowed to wait for in parallel while
    /// iterating towards the closest nodes to a target. Defaults to
    /// `ALPHA_VALUE`.
    ///
    /// This only controls the level of parallelism of an iterative query, not
    /// the level of parallelism of a query to a fixed set of peers.
    ///
    /// When used with [`KademliaConfig::disjoint_query_paths`] it equals
    /// the amount of disjoint paths used.
    pub fn set_parallelism(&mut self, parallelism: NonZeroUsize) -> &mut Self {
        self.query_config.parallelism = parallelism;
        self
    }

    /// Require iterative queries to use disjoint paths for increased resiliency
    /// in the presence of potentially adversarial nodes.
    ///
    /// When enabled the number of disjoint paths used equals the configured
    /// parallelism.
    ///
    /// See the S/Kademlia paper for more information on the high level design
    /// as well as its security improvements.
    pub fn disjoint_query_paths(&mut self, enabled: bool) -> &mut Self {
        self.query_config.disjoint_query_paths = enabled;
        self
    }

    /// Sets the interval for Provider cleanup.
    ///
    /// The provider records will be cleaned up per the interval.The default is 1
    /// hour.
    ///
    pub fn set_cleanup_interval(&mut self, interval: Duration) -> &mut Self {
        self.cleanup_interval = interval;
        self
    }

    /// Sets the TTL for stored records.
    ///
    /// The TTL should be significantly longer than the (re-)publication
    /// interval, to avoid premature expiration of records. The default is 36
    /// hours.
    ///
    /// `None` means records never expire.
    ///
    /// Does not apply to provider records.
    pub fn set_record_ttl(&mut self, record_ttl: Option<Duration>) -> &mut Self {
        self.record_ttl = record_ttl;
        self
    }

    /// Sets the (re-)replication interval for stored records.
    ///
    /// Periodic replication of stored records ensures that the records
    /// are always replicated to the available nodes closest to the key in the
    /// context of DHT topology changes (i.e. nodes joining and leaving), thus
    /// ensuring persistence until the record expires. Replication does not
    /// prolong the regular lifetime of a record (for otherwise it would live
    /// forever regardless of the configured TTL). The expiry of a record
    /// is only extended through re-publication.
    ///
    /// This interval should be significantly shorter than the publication
    /// interval, to ensure persistence between re-publications. The default
    /// is 1 hour.
    ///
    /// `None` means that stored records are never re-replicated.
    ///
    /// Does not apply to provider records.
    pub fn set_replication_interval(&mut self, interval: Option<Duration>) -> &mut Self {
        self.record_replication_interval = interval;
        self
    }

    /// Sets the (re-)publication interval of stored records.
    ///
    /// Records persist in the DHT until they expire. By default, published
    /// records are re-published in regular intervals for as long as the record
    /// exists in the local storage of the original publisher, thereby extending
    /// the records lifetime.
    ///
    /// This interval should be significantly shorter than the record TTL, to
    /// ensure records do not expire prematurely. The default is 24 hours.
    ///
    /// `None` means that stored records are never automatically re-published.
    ///
    /// Does not apply to provider records.
    pub fn set_publication_interval(&mut self, interval: Option<Duration>) -> &mut Self {
        self.record_publication_interval = interval;
        self
    }

    /// Sets the TTL for provider records.
    ///
    /// `None` means that stored provider records never expire.
    ///
    /// Must be significantly larger than the provider publication interval.
    pub fn set_provider_record_ttl(&mut self, ttl: Option<Duration>) -> &mut Self {
        self.provider_record_ttl = ttl;
        self
    }

    /// Sets the interval at which provider records for keys provided
    /// by the local node are re-published.
    ///
    /// `None` means that stored provider records are never automatically
    /// re-published.
    ///
    /// Must be significantly less than the provider record TTL.
    pub fn set_provider_publication_interval(&mut self, interval: Option<Duration>) -> &mut Self {
        self.provider_publication_interval = interval;
        self
    }

    /// Sets the amount of time to keep connections alive when they're idle.
    pub fn set_connection_idle_timeout(&mut self, duration: Duration) -> &mut Self {
        self.connection_idle_timeout = duration;
        self
    }

    /// Modifies the maximum allowed size of individual Kademlia packets.
    ///
    /// It might be necessary to increase this value if trying to put large
    /// records.
    pub fn set_max_packet_size(&mut self, size: usize) -> &mut Self {
        self.protocol_config.set_max_packet_size(size);
        self
    }

    /// Sets the k-bucket insertion strategy for the Kademlia routing table.
    pub fn set_kbucket_inserts(&mut self, inserts: KademliaBucketInserts) -> &mut Self {
        self.kbucket_inserts = inserts;
        self
    }
}

/// KadPoster is used to generate ProtocolEvent to Kad main loop.
#[derive(Clone, Debug)]
pub(crate) struct KadPoster(mpsc::UnboundedSender<ProtocolEvent>);

impl KadPoster {
    pub(crate) fn new(tx: mpsc::UnboundedSender<ProtocolEvent>) -> Self {
        Self(tx)
    }

    pub(crate) async fn post(&mut self, event: ProtocolEvent) -> Result<()> {
        let r = self.0.send(event).await?;
        Ok(r)
    }
}

impl<TStore> Kademlia<TStore>
    where
            for<'a> TStore: RecordStore<'a> + Send + 'static
{
    /// Creates a new `Kademlia` network behaviour with a default configuration.
    pub fn new(id: PeerId, store: TStore) -> Self {
        Self::with_config(id, store, Default::default())
    }

    // /// Get the protocol name of this kademlia instance.
    // pub fn protocol_name(&self) -> &[u8] {
    //     self.protocol_config.protocol_name()
    // }

    /// Creates a new `Kademlia` network behaviour with the given configuration.
    pub fn with_config(id: PeerId, store: TStore, config: KademliaConfig) -> Self {
        let local_key = kbucket::Key::new(id.clone());

        let put_record_job = config
            .record_replication_interval
            .or(config.record_publication_interval)
            .map(|interval| PutRecordJob::new(
                id.clone(),
                interval,
                config.record_publication_interval,
                config.record_ttl,
            ));

        let add_provider_job = config
            .provider_publication_interval
            .map(AddProviderJob::new);

        let (event_tx, event_rx) = mpsc::unbounded();
        let (control_tx, control_rx) = mpsc::unbounded();

        Kademlia {
            store,
            swarm: None,
            event_rx,
            event_tx,
            control_tx,
            control_rx,
            kbuckets: KBucketsTable::new(local_key, config.kbucket_pending_timeout),
            kbucket_inserts: config.kbucket_inserts,
            protocol_config: config.protocol_config,
            query_config: config.query_config,
            messengers: None,
            connected_peers: Default::default(),
            provider_timer_handle: None,
            add_provider_job,
            put_record_job,
            cleanup_interval: config.cleanup_interval,
            record_ttl: config.record_ttl,
            provider_record_ttl: config.provider_record_ttl,
            connection_idle_timeout: config.connection_idle_timeout,
            local_addrs: FnvHashSet::default(),
        }
    }

    // Returns a copied instance of Kad poster.
    fn get_poster(&self) -> KadPoster {
        KadPoster::new(self.event_tx.clone())
    }

/*
    /// Adds a known listen address of a peer participating in the DHT to the
    /// routing table.
    ///
    /// Explicitly adding addresses of peers serves two purposes:
    ///
    ///   1. In order for a node to join the DHT, it must know about at least
    ///      one other node of the DHT.
    ///
    ///   2. When a remote peer initiates a connection and that peer is not
    ///      yet in the routing table, the `Kademlia` behaviour must be
    ///      informed of an address on which that peer is listening for
    ///      connections before it can be added to the routing table
    ///      from where it can subsequently be discovered by all peers
    ///      in the DHT.
    ///
    /// If the routing table has been updated as a result of this operation,
    /// a [`KademliaEvent::RoutingUpdated`] event is emitted.
    pub fn add_address(&mut self, peer: &PeerId, address: Multiaddr) -> RoutingUpdate {
        let key = kbucket::Key::new(peer.clone());
        match self.kbuckets.entry(&key) {
            kbucket::Entry::Present(mut entry, _) => {
                if entry.value().insert(address) {
                    self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                        KademliaEvent::RoutingUpdated {
                            peer: peer.clone(),
                            addresses: entry.value().clone(),
                            old_peer: None,
                        }
                    ))
                }
                RoutingUpdate::Success
            }
            kbucket::Entry::Pending(mut entry, _) => {
                entry.value().insert(address);
                RoutingUpdate::Pending
            }
            kbucket::Entry::Absent(entry) => {
                let addresses = Addresses::new(address);
                let status =
                    if self.connected_peers.contains(peer) {
                        NodeStatus::Connected
                    } else {
                        NodeStatus::Disconnected
                    };
                match entry.insert(addresses.clone(), status) {
                    kbucket::InsertResult::Inserted => {
                        self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                            KademliaEvent::RoutingUpdated {
                                peer: peer.clone(),
                                addresses,
                                old_peer: None,
                            }
                        ));
                        RoutingUpdate::Success
                    },
                    kbucket::InsertResult::Full => {
                        log::debug!("Bucket full. Peer not added to routing table: {}", peer);
                        RoutingUpdate::Failed
                    },
                    kbucket::InsertResult::Pending { disconnected } => {
                        self.queued_events.push_back(NetworkBehaviourAction::DialPeer {
                            peer_id: disconnected.into_preimage(),
                            condition: DialPeerCondition::Disconnected
                        });
                        RoutingUpdate::Pending
                    },
                }
            },
            kbucket::Entry::SelfEntry => RoutingUpdate::Failed,
        }
    }

    /// Removes an address of a peer from the routing table.
    ///
    /// If the given address is the last address of the peer in the
    /// routing table, the peer is removed from the routing table
    /// and `Some` is returned with a view of the removed entry.
    /// The same applies if the peer is currently pending insertion
    /// into the routing table.
    ///
    /// If the given peer or address is not in the routing table,
    /// this is a no-op.
    pub fn remove_address(&mut self, peer: &PeerId, address: &Multiaddr)
                          -> Option<kbucket::EntryView<kbucket::Key<PeerId>, Addresses>>
    {
        let key = kbucket::Key::new(peer.clone());
        match self.kbuckets.entry(&key) {
            kbucket::Entry::Present(mut entry, _) => {
                if entry.value().remove(address).is_err() {
                    Some(entry.remove()) // it is the last address, thus remove the peer.
                } else {
                    None
                }
            }
            kbucket::Entry::Pending(mut entry, _) => {
                if entry.value().remove(address).is_err() {
                    Some(entry.remove()) // it is the last address, thus remove the peer.
                } else {
                    None
                }
            }
            kbucket::Entry::Absent(..) | kbucket::Entry::SelfEntry => {
                None
            }
        }
    }
*/
    /// Removes a peer from the routing table.
    ///
    /// Returns `None` if the peer was not in the routing table,
    /// not even pending insertion.
    pub fn remove_peer(&mut self, peer: &PeerId)
                       -> Option<kbucket::EntryView<kbucket::Key<PeerId>, Addresses>>
    {
        let key = kbucket::Key::new(peer.clone());
        match self.kbuckets.entry(&key) {
            kbucket::Entry::Present(entry, _) => {
                Some(entry.remove())
            }
            kbucket::Entry::Pending(entry, _) => {
                Some(entry.remove())
            }
            kbucket::Entry::Absent(..) | kbucket::Entry::SelfEntry => {
                None
            }
        }
    }

    /// Returns an iterator over all non-empty buckets in the routing table.
    fn kbuckets(&mut self)
                    -> impl Iterator<Item = kbucket::KBucketRef<'_, kbucket::Key<PeerId>, Addresses>>
    {
        self.kbuckets.iter().filter(|b| !b.is_empty())
    }

    /// Returns the k-bucket for the distance to the given key.
    ///
    /// Returns `None` if the given key refers to the local key.
    pub fn kbucket<K>(&mut self, key: K)
                      -> Option<kbucket::KBucketRef<'_, kbucket::Key<PeerId>, Addresses>>
        where
            K: Borrow<[u8]> + Clone
    {
        self.kbuckets.bucket(&kbucket::Key::new(key))
    }

    /// Gets a mutable reference to the record store.
    pub fn store_mut(&mut self) -> &mut TStore {
        &mut self.store
    }

    // prepare and generate a IterativeQuery for iterative query.
    fn prepare_iterative_query(&mut self, qt: QueryType, key: record::Key) -> IterativeQuery
    {
        let local_id = self.kbuckets.self_key().preimage().clone();
        let target = kbucket::Key::new(key.clone());
        let seeds = self.kbuckets.closest_keys(&target).into_iter().collect();

        let query = IterativeQuery::new(qt, key,
                                        self.messengers.clone().expect("must be Some"),
                                        local_id, &self.query_config,
                                        seeds, self.get_poster());

        query
    }

    /// Initiates an iterative lookup for the closest peers to the given key.
    fn get_closest_peers<F>(&mut self, key: record::Key, f: F)
        where
            F: FnOnce(Result<Vec<KadPeer>>) + Send + 'static
    {
        let mut q = self.prepare_iterative_query(QueryType::GetClosestPeers, key);

        q.run(|r| {
            f(r.and_then(|r| r.closest_peers.ok_or(KadError::NotFound)));
        });
    }

    /// Performs an iterative lookup for the closest peers to the given key.
    ///
    /// The result of this operation is delivered into the callback
    /// Fn(Result<Option<KadPeer>>).
    fn find_peer<F>(&mut self, key: record::Key, f: F)
        where
            F: FnOnce(Result<KadPeer>) + Send + 'static
    {
        let mut q = self.prepare_iterative_query(QueryType::FindPeer, key);

        q.run(|r| {
            f(r.and_then(|r| r.found_peer.ok_or(KadError::NotFound)));
        });
    }

    /// Performs a lookup for providers of a value to the given key.
    ///
    /// The result of this operation is delivered into the callback
    /// Fn(Result<Vec<KadPeer>>).
    fn get_providers<F>(&mut self, key: record::Key, count: usize, f: F)
        where
            F: FnOnce(Result<Vec<KadPeer>>) + Send + 'static
    {
        let provider_peers = self.provider_peers(&key, None);

        if provider_peers.len() >= count {
            // ok, we have enough providers for this key, simply return
            f(Ok(provider_peers));
        } else {
            let remaining = count - provider_peers.len();
            let mut q = self.prepare_iterative_query(QueryType::GetProviders { count: remaining, local: Some(provider_peers) }, key);

            q.run(|r|{
                f(r.and_then(|r| r.providers.ok_or(KadError::NotFound)));
            });
        }

    }

    /// Performs a lookup of a record to the given key.
    ///
    /// The result of this operation is delivered into the callback
    /// Fn(Result<Vec<PeerRecord>>).
    fn get_record<F>(&mut self, key: record::Key, f: F)
        where
            F: FnOnce(Result<Vec<PeerRecord>>) + Send + 'static
    {
        let quorum = self.query_config.replication_factor.get();
        let mut records = Vec::with_capacity(quorum);

        if let Some(record) = self.store.get(&key) {
            if record.is_expired(Instant::now()) {
                self.store.remove(&key)
            } else {
                records.push(PeerRecord { peer: None, record: record.into_owned()});
            }
        }

        if records.len() >= quorum {
            // ok, we have enough, simply return
            f(Ok(records));
        } else {
            let needed = quorum - records.len();
            let mut q = self.prepare_iterative_query(QueryType::GetRecord { quorum_needed: needed, local: Some(records) }, key);
            q.run(|r|{
                f(r.and_then(|r| r.records.ok_or(KadError::NotFound)));
            });
        }
    }

    /// Stores a record in the DHT.
    ///
    /// The record is always stored locally with the given expiration. If the record's
    /// expiration is `None`, the common case, it does not expire in local storage
    /// but is still replicated with the configured record TTL. To remove the record
    /// locally and stop it from being re-published in the DHT, see [`Kademlia::remove_record`].
    ///
    /// After the initial publication of the record, it is subject to (re-)replication
    /// and (re-)publication as per the configured intervals. Periodic (re-)publication
    /// does not update the record's expiration in local storage, thus a given record
    /// with an explicit expiration will always expire at that instant and until then
    /// is subject to regular (re-)replication and (re-)publication.
    ///
    /// The result of this operation is delivered into the callback
    /// Fn(Result<()>).
    fn put_record<F>(&mut self, key: record::Key, value: Vec<u8>, f: F)
        where
            F: FnOnce(Result<()>) + Send + 'static
    {
        // TODO: probably we should check if there is a old record with the same key?

        let mut record = Record {
            key,
            value,
            publisher: None,
            expires: None,
        };

        record.publisher = Some(self.kbuckets.self_key().preimage().clone());
        if let Err(e) = self.store.put(record.clone()) {
            f(Err(e));
            return;
        }
        record.expires = record.expires.or_else(||
            self.record_ttl.map(|ttl| Instant::now() + ttl));
        //let quorum = self.queries.config().replication_factor.get();
        let alpha_value = self.query_config.parallelism;
        let messengers = self.messengers.clone().expect("must be Some");
        // initialte the iterative lookup for closest peers, which can be used to publish the record
        self.get_closest_peers(record.key.clone(), move |peers| {
            if let Err(e) = peers {
                f(Err(e));
            } else {
                let mut limiter = TaskLimiter::new(alpha_value);
                task::spawn(async move {
                    for peer in peers.unwrap() {
                        let record = record.clone();
                        let mut messengers = messengers.clone();

                        // TODO: quorum of PutValue???
                        limiter.run(async move {
                            if let Ok(mut ms) = messengers.get_messenger(&peer.node_id).await {
                                if ms.send_put_value(record).await.is_ok() {
                                    messengers.put_messenger(ms).await;
                                }
                                // otherwise, ms will be dropped here, a task will be spawned to close
                                // the inner substream...
                            }
                        }).await;
                    }
                    let c = limiter.wait().await;
                    log::info!("record announced to total {} peers", c);
                    f(Ok(()))
                });
            }
        });
    }

    /// Removes the record with the given key from _local_ storage,
    /// if the local node is the publisher of the record.
    ///
    /// Has no effect if a record for the given key is stored locally but
    /// the local node is not a publisher of the record.
    ///
    /// This is a _local_ operation. However, it also has the effect that
    /// the record will no longer be periodically re-published, allowing the
    /// record to eventually expire throughout the DHT.
    fn remove_record(&mut self, key: &record::Key) {
        if let Some(r) = self.store.get(key) {
            if r.publisher.as_ref() == Some(self.kbuckets.self_key().preimage()) {
                self.store.remove(key)
            }
        }
    }

    /// Bootstraps the local node to join the DHT.
    ///
    /// Bootstrapping is a multi-step operation that starts with a lookup of the local node's
    /// own ID in the DHT. This introduces the local node to the other nodes
    /// in the DHT and populates its routing table with the closest neighbours.
    ///
    /// Subsequently, all buckets farther from the bucket of the closest neighbour are
    /// refreshed by initiating an additional bootstrapping query for each such
    /// bucket with random keys.
    ///
    /// Returns `Ok` if bootstrapping has been initiated with a self-lookup, providing the
    /// `QueryId` for the entire bootstrapping process. The progress of bootstrapping is
    /// reported via [`KademliaEvent::QueryResult{QueryResult::Bootstrap}`] events,
    /// with one such event per bootstrapping query.
    ///
    /// Returns `Err` if bootstrapping is impossible due an empty routing table.
    ///
    /// > **Note**: Bootstrapping requires at least one node of the DHT to be known.
    /// > See [`Kademlia::add_address`].
    fn bootstrap(&mut self) {
        let local_key = self.kbuckets.self_key().clone();
        let key = local_key.into_preimage().into_bytes();

        self.get_closest_peers(key.into(), |_| {});
    }

    /// Performs publishing as a provider of a value for the given key.
    ///
    /// This operation publishes a provider record with the given key and
    /// identity of the local node to the peers closest to the key, thus establishing
    /// the local node as a provider.
    ///
    /// The publication of the provider records is periodically repeated as per the
    /// configured interval, to renew the expiry and account for changes to the DHT
    /// topology. A provider record may be removed from local storage and
    /// thus no longer re-published by calling [`Kademlia::stop_providing`].
    ///
    /// In contrast to the standard Kademlia push-based model for content distribution
    /// implemented by [`Kademlia::put_record`], the provider API implements a
    /// pull-based model that may be used in addition or as an alternative.
    /// The means by which the actual value is obtained from a provider is out of scope
    /// of the libp2p Kademlia provider API.
    ///
    /// The result of this operation is delivered into the callback
    /// Fn(Result<()>).
    fn start_providing<F>(&mut self, key: record::Key, f: F)
        where
            F: FnOnce(Result<()>) + Send + 'static
    {
        // Note: We store our own provider records locally without local addresses
        // to avoid redundant storage and outdated addresses. Instead these are
        // acquired on demand when returning a `ProviderRecord` for the local node.
        let local_addrs = Vec::new();
        let record = ProviderRecord::new(
            key.clone(),
            self.kbuckets.self_key().preimage().clone(),
            local_addrs);
        if let Err(e) = self.store.add_provider(record.clone()) {
            f(Err(e));
            return;
        }
        let alpha_value = self.query_config.parallelism;
        let messengers = self.messengers.clone().expect("must be Some");

        // initialte the iterative lookup for closest peers, which can be used to publish the record
        self.get_closest_peers(key, move |peers| {
            if let Err(e) = peers {
                f(Err(e));
            } else {
                let mut limiter = TaskLimiter::new(alpha_value);
                task::spawn(async move {
                    for peer in peers.unwrap() {
                        let record = record.clone();
                        let mut messengers = messengers.clone();

                        // TODO: quorum of AddProvider???
                        limiter.run(async move {
                            if let Ok(mut ms) = messengers.get_messenger(&peer.node_id).await {
                                if ms.send_add_provider(record).await.is_ok() {
                                    messengers.put_messenger(ms).await;
                                }
                                // otherwise, ms will be dropped here, a task will be spawned to close
                                // the inner substream...
                            }
                        }).await;
                    }
                    let c = limiter.wait().await;
                    log::info!("provider announced to total {} peers", c);
                    f(Ok(()))
                });
            }
        });
        // let target = kbucket::Key::new(key.clone());
        // let peers = self.kbuckets.closest_keys(&target);
        // let context = AddProviderContext::Publish;
        // let info = QueryInfo::AddProvider {
        //     context,
        //     key,
        //     //phase: AddProviderPhase::GetClosestPeers
        //     // TODO
        //     provider_id: PeerId::random(),
        //     external_addresses: vec![],
        //     get_closest_peers_stats: QueryStats::empty()
        // };
        // let inner = QueryInner::new(info);
        // let id = self.queries.add_iter_closest(target.clone(), peers, inner);
        // Ok(id)
    }

    /// Stops the local node from announcing that it is a provider for the given key.
    ///
    /// This is a local operation. The local node will still be considered as a
    /// provider for the key by other nodes until these provider records expire.
    fn stop_providing(&mut self, key: &record::Key) {
        self.store.remove_provider(key, self.kbuckets.self_key().preimage());
    }

    /// Finds the closest peers to a `target` in the context of a request by
    /// the `source` peer, such that the `source` peer is never included in the
    /// result.
    fn find_closest<T: Clone>(&mut self, target: &kbucket::Key<T>, source: &PeerId) -> Vec<KadPeer> {
        if target == self.kbuckets.self_key() {
            Vec::new()
        } else {
            self.kbuckets
                .closest(target)
                .filter(|e| e.node.key.preimage() != source)
                .take(self.query_config.replication_factor.get())
                .map(KadPeer::from)
                .collect()
        }
    }

    /// Collects all peers who are known to be providers of the value for a given `Multihash`.
    fn provider_peers(&mut self, key: &record::Key, source: Option<&PeerId>) -> Vec<KadPeer> {
        let kbuckets = &mut self.kbuckets;
        let connected = &mut self.connected_peers;
        let local_addrs = &self.local_addrs;
        self.store.providers(key)
            .into_iter()
            .filter_map(move |p|
                // &p.provider != source, kingwel makes the change
                if source.map_or(true, |pid|pid != &p.provider) {
                    let node_id = p.provider;
                    let multiaddrs = p.addresses;
                    let connection_ty = if connected.contains(&node_id) {
                        KadConnectionType::Connected
                    } else {
                        KadConnectionType::NotConnected
                    };
                    if multiaddrs.is_empty() {
                        // The provider is either the local node and we fill in
                        // the local addresses on demand, or it is a legacy
                        // provider record without addresses, in which case we
                        // try to find addresses in the routing table, as was
                        // done before provider records were stored along with
                        // their addresses.
                        if &node_id == kbuckets.self_key().preimage() {
                            Some(local_addrs.iter().cloned().collect::<Vec<_>>())
                        } else {
                            let key = kbucket::Key::new(node_id.clone());
                            kbuckets.entry(&key).view().map(|e| e.node.value.clone().into_vec())
                        }
                    } else {
                        Some(multiaddrs)
                    }
                        .map(|multiaddrs| {
                            KadPeer {
                                node_id,
                                multiaddrs,
                                connection_ty,
                            }
                        })
                } else {
                    None
                })
            .take(self.query_config.replication_factor.get())
            .collect()
    }
/*
    /// Starts an iterative `ADD_PROVIDER` query for the given key.
    fn start_add_provider(&mut self, key: record::Key, context: AddProviderContext) {
        let info = QueryInfo::AddProvider {
            context,
            key: key.clone(),
            //phase: AddProviderPhase::GetClosestPeers
            provider_id: PeerId::random(),
            external_addresses: vec![],
            get_closest_peers_stats: QueryStats::empty()
        };
        let target = kbucket::Key::new(key);
        let peers = self.kbuckets.closest_keys(&target);
        let inner = QueryInner::new(info);
        self.queries.add_iter_closest(target.clone(), peers, inner);
    }

    /// Starts an iterative `PUT_VALUE` query for the given record.
    fn start_put_record(&mut self, record: Record, context: PutRecordContext) {
        let quorum = self.query_config.replication_factor;
        let target = kbucket::Key::new(record.key.clone());
        let peers = self.kbuckets.closest_keys(&target);
        let info = QueryInfo::PutRecord {
            record, quorum, context//, phase: PutRecordPhase::GetClosestPeers
        };
        let inner = QueryInner::new(info);
        self.queries.add_iter_closest(target.clone(), peers, inner);
    }
*/
    /// Updates the routing table with a new connection status and address of a peer.
    fn connection_updated(&mut self, peer: PeerId, address: Option<Multiaddr>, new_status: NodeStatus) {
        let key = kbucket::Key::new(peer.clone());
        match self.kbuckets.entry(&key) {
            kbucket::Entry::Present(mut entry, old_status) => {
                if let Some(address) = address {
                    if entry.value().insert(address) {
                        // self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                        //     KademliaEvent::RoutingUpdated {
                        //         peer,
                        //         addresses: entry.value().clone(),
                        //         old_peer: None,
                        //     }
                        // ))
                    }
                }
                if old_status != new_status {
                    entry.update(new_status);
                }
            },

            kbucket::Entry::Pending(mut entry, old_status) => {
                if let Some(address) = address {
                    entry.value().insert(address);
                }
                if old_status != new_status {
                    entry.update(new_status);
                }
            },

            kbucket::Entry::Absent(entry) => {
                // Only connected nodes with a known address are newly inserted.
                if new_status != NodeStatus::Connected {
                    return
                }
                match (address, self.kbucket_inserts) {
                    (None, _) => {
                        // self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                        //     KademliaEvent::UnroutablePeer { peer }
                        // ));
                    }
                    (Some(a), KademliaBucketInserts::Manual) => {
                        // self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                        //     KademliaEvent::RoutablePeer { peer, address: a }
                        // ));
                    }
                    (Some(a), KademliaBucketInserts::OnConnected) => {
                        let addresses = Addresses::new(a);
                        match entry.insert(addresses.clone(), new_status) {
                            kbucket::InsertResult::Inserted => {
                                let event = KademliaEvent::RoutingUpdated {
                                    peer: peer.clone(),
                                    addresses,
                                    old_peer: None,
                                };
                                // self.queued_events.push_back(
                                //     NetworkBehaviourAction::GenerateEvent(event));
                            },
                            kbucket::InsertResult::Full => {
                                log::debug!("Bucket full. Peer not added to routing table: {}", peer);
                                let address = addresses.first().clone();
                                // self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                                //     KademliaEvent::RoutablePeer { peer, address }
                                // ));
                            },
                            kbucket::InsertResult::Pending { disconnected } => {
                                debug_assert!(!self.connected_peers.contains(disconnected.preimage()));
                                let address = addresses.first().clone();
                                // self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                                //     KademliaEvent::PendingRoutablePeer { peer, address }
                                // ));
                                // self.queued_events.push_back(NetworkBehaviourAction::DialPeer {
                                //     peer_id: disconnected.into_preimage(),
                                //     condition: DialPeerCondition::Disconnected
                                // })
                            },
                        }
                    }
                }
            },
            _ => {}
        }
    }

    /// Processes a record received from a peer.
    fn handle_put_record(
        &mut self,
        _source: PeerId,
        mut record: Record
    ) -> Result<KadResponseMsg> {
        if record.publisher.as_ref() == Some(self.kbuckets.self_key().preimage()) {
            // If the (alleged) publisher is the local node, do nothing. The record of
            // the original publisher should never change as a result of replication
            // and the publisher is always assumed to have the "right" value.
            return Ok(KadResponseMsg::PutValue { key: record.key, value: record.value });
        }

        let now = Instant::now();

        // Calculate the expiration exponentially inversely proportional to the
        // number of nodes between the local node and the closest node to the key
        // (beyond the replication factor). This ensures avoiding over-caching
        // outside of the k closest nodes to a key.
        let target = kbucket::Key::new(record.key.clone());
        let num_between = self.kbuckets.count_nodes_between(&target);
        let k = self.query_config.replication_factor.get();
        let num_beyond_k = (usize::max(k, num_between) - k) as u32;
        let expiration = self.record_ttl.map(|ttl| now + exp_decrease(ttl, num_beyond_k));
        // The smaller TTL prevails. Only if neither TTL is set is the record
        // stored "forever".
        record.expires = record.expires.or(expiration).min(expiration);

        if let Some(job) = self.put_record_job.as_mut() {
            // Ignore the record in the next run of the replication
            // job, since we can assume the sender replicated the
            // record to the k closest peers. Effectively, only
            // one of the k closest peers performs a replication
            // in the configured interval, assuming a shared interval.
            job.skip(record.key.clone())
        }

        // While records received from a publisher, as well as records that do
        // not exist locally should always (attempted to) be stored, there is a
        // choice here w.r.t. the handling of replicated records whose keys refer
        // to records that exist locally: The value and / or the publisher may
        // either be overridden or left unchanged. At the moment and in the
        // absence of a decisive argument for another option, both are always
        // overridden as it avoids having to load the existing record in the
        // first place.

        if !record.is_expired(now) {
            // The record is cloned because of the weird libp2p protocol
            // requirement to send back the value in the response, although this
            // is a waste of resources.
            match self.store.put(record.clone()) {
                Ok(()) => log::debug!("Record stored: {:?}; {} bytes", record.key, record.value.len()),
                Err(e) => {
                    log::info!("Record not stored: {:?}", e);
                    return Err(e);
                }
            }
        }

        // The remote receives a [`KademliaHandlerIn::PutRecordRes`] even in the
        // case where the record is discarded due to being expired. Given that
        // the remote sent the local node a [`ProtocolEvent::PutRecord`]
        // request, the remote perceives the local node as one node among the k
        // closest nodes to the target. In addition returning
        // [`KademliaHandlerIn::PutRecordRes`] does not reveal any internal
        // information to a possibly malicious remote node.
        Ok(KadResponseMsg::PutValue {
                key: record.key,
                value: record.value,
        })
    }

    /// Processes a provider record received from a peer.
    fn handle_add_provider(&mut self, key: record::Key, provider: KadPeer) {
        if &provider.node_id != self.kbuckets.self_key().preimage() {
            let record = ProviderRecord {
                key,
                provider: provider.node_id,
                expires: self.provider_record_ttl.map(|ttl| Instant::now() + ttl),
                addresses: provider.multiaddrs,
            };
            if let Err(e) = self.store.add_provider(record) {
                log::info!("Provider record not stored: {:?}", e);
            }
        }
    }

    /// Get the protocol handler of Kademlia, swarm will call "handle" func after stream negotiation.
    pub fn handler(&self) -> KadProtocolHandler {
        KadProtocolHandler::new(self.protocol_config.clone(), self.get_poster())
    }
    /// Get the controller of Kademlia, which can be used to manipulate the Kad-DHT.
    pub fn control(&self) -> Control {
        Control::new(self.control_tx.clone())
    }

    /// Start the main message loop of Kademlia.
    pub fn start(mut self, swarm: SwarmControl) {
        self.messengers = Some(MessengerManager::new(swarm.clone(), self.protocol_config.clone()));
        self.swarm = Some(swarm);

        self.bootstrap();

        // start timer task, which would generate ProtocolEvent::Timer to kad main loop
        log::info!("starting provider timer task...");
        let interval = self.cleanup_interval;
        let mut poster = self.get_poster();
        let h = task::spawn(async move {
            loop {
                task::sleep(interval).await;
                let _= poster.post(ProtocolEvent::ProviderCleanupTimer).await;
            }
        });

        self.provider_timer_handle = Some(h);

        // well, self 'move' explicitly,
        let mut kad = self;
        task::spawn(async move {
            let _ = kad.process_loop().await;
        });
    }


    /// Message Process Loop.
    async fn process_loop(&mut self) -> Result<()> {
        let result = self.next().await;
        //
        // if !self.peer_rx.is_terminated() {
        //     self.peer_rx.close();
        //     while self.peer_rx.next().await.is_some() {
        //         // just drain
        //     }
        // }
        //
        // if !self.event_rx.is_terminated() {
        //     self.event_rx.close();
        //     while self.event_rx.next().await.is_some() {
        //         // just drain
        //     }
        // }
        //
        // if !self.control_rx.is_terminated() {
        //     self.control_rx.close();
        //     while let Some(cmd) = self.control_rx.next().await {
        //         match cmd {
        //             ControlCommand::Publish(_, reply) => {
        //                 let _ = reply.send(());
        //             }
        //             ControlCommand::Subscribe(_, reply) => {
        //                 let _ = reply.send(None);
        //             }
        //             ControlCommand::Ls(reply) => {
        //                 let _ = reply.send(Vec::new());
        //             }
        //             ControlCommand::GetPeers(_, reply) => {
        //                 let _ = reply.send(Vec::new());
        //             }
        //         }
        //     }
        // }
        //
        // if !self.cancel_rx.is_terminated() {
        //     self.cancel_rx.close();
        //     while self.cancel_rx.next().await.is_some() {
        //         // just drain
        //     }
        // }
        //
        // self.drop_all_peers();
        // self.drop_all_my_topics();
        // self.drop_all_topics();

        result
    }

    async fn next(&mut self) -> Result<()> {
        loop {
            select! {
                // cmd = self.peer_rx.next() => {
                //     self.handle_peer_event(cmd).await;
                // }
                evt = self.event_rx.next() => {
                    self.handle_events(evt).await?;
                }
                cmd = self.control_rx.next() => {
                    self.on_control_command(cmd).await?;
                }
            }
        }
    }

    // Called when new peer is connected.
    async fn handle_peer_connected(&mut self, peer_id: PeerId) {
        // the peer id might have existed in the hashset, don't care too much
        self.connected_peers.insert(peer_id);
    }

    // Called when a peer is disconnected.
    async fn handle_peer_disconnected(&mut self, peer_id: PeerId) {
        // remove the peer from the hashset
        self.connected_peers.remove(&peer_id);
        // TODO: figure out what it shall do
        self.connection_updated(peer_id, None, NodeStatus::Disconnected);
    }

    // handle a new Kad peer is found.
    async fn handle_peer_found(&mut self, peer_id: PeerId, queried: bool) {
        // TODO
    }

    // handle a Kad peer is dead.
    async fn handle_peer_stopped(&mut self, peer_id: PeerId) {
        // TODO
    }

    // Handle Kad events sent from protocol handler.
    async fn handle_events(&mut self, msg: Option<ProtocolEvent>) -> Result<()> {
        match msg {
            Some(ProtocolEvent::PeerConnected(peer_id)) => {
                self.handle_peer_connected(peer_id).await;
                Ok(())
            }
            Some(ProtocolEvent::PeerDisconnected(peer_id)) => {
                self.handle_peer_disconnected(peer_id).await;
                Ok(())
            }
            Some(ProtocolEvent::KadPeerFound(peer_id, queried)) => {
                self.handle_peer_found(peer_id, queried).await;
                Ok(())
            }
            Some(ProtocolEvent::KadPeerStopped(peer_id)) => {
                self.handle_peer_stopped(peer_id).await;
                Ok(())
            }
            Some(ProtocolEvent::KadRequest {
                request,
                source,
                reply
            }) => {
                self.handle_kad_request(request, source, reply);
                Ok(())
            }
            Some(ProtocolEvent::ProviderCleanupTimer) => {
                self.handle_provider_cleanup();
                Ok(())
            }
            Some(_) => {
                Ok(())
            }
            None => Err(KadError::Closed(1)),
        }
    }

    // Handles Kad request messages. ProtoBuf message decoded by handler.
    fn handle_kad_request(&mut self, request: KadRequestMsg, source: PeerId, reply: oneshot::Sender<Result<Option<KadResponseMsg>>>) {
        let response = match request {
            KadRequestMsg::Ping => {
                // respond with the request message
                Ok(Some(KadResponseMsg::Pong))
            }
            KadRequestMsg::FindNode { key } => {
                let closer_peers = self.find_closest(&kbucket::Key::new(key), &source);
                Ok(Some(KadResponseMsg::FindNode {
                    closer_peers
                }))
            }
            KadRequestMsg::AddProvider { key, provider } => {
                // Only accept a provider record from a legitimate peer.
                if provider.node_id != source {
                    log::info!("received provider from wrong peer {:?}", source);
                    Err(KadError::InvalidSource(source))
                } else {
                    self.handle_add_provider(key, provider);
                    // AddProvider doesn't require a response
                    Ok(None)
                }
            }
            KadRequestMsg::GetProviders { key } => {
                let provider_peers = self.provider_peers(&key, Some(&source));
                let closer_peers = self.find_closest(&kbucket::Key::new(key), &source);
                Ok(Some(KadResponseMsg::GetProviders {
                        closer_peers,
                        provider_peers,
                }))

            }
            KadRequestMsg::GetValue { key } => {
                // Lookup the record locally.
                let record = match self.store.get(&key) {
                    Some(record) => {
                        if record.is_expired(Instant::now()) {
                            self.store.remove(&key);
                            None
                        } else {
                            Some(record.into_owned())
                        }
                    },
                    None => None
                };

                let closer_peers = self.find_closest(&kbucket::Key::new(key), &source);
                Ok(Some(KadResponseMsg::GetValue {
                        record,
                        closer_peers,
                }))
            }
            KadRequestMsg::PutValue { record } => {
                self.handle_put_record(source, record).and_then(|r|Ok(Some(r)))
            }
        };

        let _ = reply.send(response);
    }

    fn handle_provider_cleanup(&mut self) {
        // try to cleanup provider records
        let now = Instant::now();
        log::info!("handle_provider_cleanup, invoked at {:?}", now);

        let provider_records = self.store.provided()
            .filter(|r| r.is_expired(now))
            .map(|r| r.into_owned())
            .collect::<Vec<_>>();

        provider_records.into_iter().for_each(|r|{
            self.store.remove_provider(&r.key, &r.provider);
        });
    }

    // Process publish or subscribe command.
    async fn on_control_command(&mut self, cmd: Option<ControlCommand>) -> Result<()> {
        match cmd {
            Some(ControlCommand::Lookup(key, reply)) => {
                self.get_closest_peers(key, |r| {
                    let _ = reply.send(r);
                });
            }
            Some(ControlCommand::FindPeer(peer_id, reply)) => {
                self.find_peer(peer_id.into_bytes().into(), |r| {
                    let _ = reply.send(r);
                });
            }
            Some(ControlCommand::FindProviders(key, count, reply)) => {
                self.get_providers(key, count, |r| {
                    let _ = reply.send(r);
                });
            }
            Some(ControlCommand::Providing(key, reply)) => {
                self.start_providing(key, |r| {
                    let _ = reply.send(r);
                });
            }
            Some(ControlCommand::PutValue(key, value, reply)) => {
                self.put_record(key, value, |r| {
                    let _ = reply.send(r);
                });
            }
            Some(ControlCommand::GetValue(key, reply)) => {
                self.get_record(key, |r| {
                    let _ = reply.send(r);
                });
            }
            None => {}
        }

        Ok(())
    }
}

/// Exponentially decrease the given duration (base 2).
fn exp_decrease(ttl: Duration, exp: u32) -> Duration {
    Duration::from_secs(ttl.as_secs().checked_shr(exp).unwrap_or(0))
}




//////////////////////////////////////////////////////////////////////////////
// Events

/// The events produced by the `Kademlia` behaviour.
///
/// See [`NetworkBehaviour::poll`].
#[derive(Debug)]
pub enum KademliaEvent {

    /// The routing table has been updated with a new peer and / or
    /// address, thereby possibly evicting another peer.
    RoutingUpdated {
        /// The ID of the peer that was added or updated.
        peer: PeerId,
        /// The full list of known addresses of `peer`.
        addresses: Addresses,
        /// The ID of the peer that was evicted from the routing table to make
        /// room for the new peer, if any.
        old_peer: Option<PeerId>,
    },

    /// A peer has connected for whom no listen address is known.
    ///
    /// If the peer is to be added to the routing table, a known
    /// listen address for the peer must be provided via [`Kademlia::add_address`].
    UnroutablePeer {
        peer: PeerId
    },

    /// A connection to a peer has been established for whom a listen address
    /// is known but the peer has not been added to the routing table either
    /// because [`KademliaBucketInserts::Manual`] is configured or because
    /// the corresponding bucket is full.
    ///
    /// If the peer is to be included in the routing table, it must
    /// must be explicitly added via [`Kademlia::add_address`], possibly after
    /// removing another peer.
    ///
    /// See [`Kademlia::kbucket`] for insight into the contents of
    /// the k-bucket of `peer`.
    RoutablePeer {
        peer: PeerId,
        address: Multiaddr,
    },

    /// A connection to a peer has been established for whom a listen address
    /// is known but the peer is only pending insertion into the routing table
    /// if the least-recently disconnected peer is unresponsive, i.e. the peer
    /// may not make it into the routing table.
    ///
    /// If the peer is to be unconditionally included in the routing table,
    /// it should be explicitly added via [`Kademlia::add_address`] after
    /// removing another peer.
    ///
    /// See [`Kademlia::kbucket`] for insight into the contents of
    /// the k-bucket of `peer`.
    PendingRoutablePeer {
        peer: PeerId,
        address: Multiaddr,
    }
}


impl From<kbucket::EntryView<kbucket::Key<PeerId>, Addresses>> for KadPeer {
    fn from(e: kbucket::EntryView<kbucket::Key<PeerId>, Addresses>) -> KadPeer {
        KadPeer {
            node_id: e.node.key.into_preimage(),
            multiaddrs: e.node.value.into_vec(),
            connection_ty: match e.status {
                NodeStatus::Connected => KadConnectionType::Connected,
                NodeStatus::Disconnected => KadConnectionType::NotConnected
            }
        }
    }
}

/// The possible outcomes of [`Kademlia::add_address`].
pub enum RoutingUpdate {
    /// The given peer and address has been added to the routing
    /// table.
    Success,
    /// The peer and address is pending insertion into
    /// the routing table, if a disconnected peer fails
    /// to respond. If the given peer and address ends up
    /// in the routing table, [`KademliaEvent::RoutingUpdated`]
    /// is eventually emitted.
    Pending,
    /// The routing table update failed, either because the
    /// corresponding bucket for the peer is full and the
    /// pending slot(s) are occupied, or because the given
    /// peer ID is deemed invalid (e.g. refers to the local
    /// peer ID).
    Failed,
}


///////////////////////////////////////////
#[derive(Clone)]
pub(crate) struct MessengerManager {
    swarm: SwarmControl,
    config: KademliaProtocolConfig,
    cache: Arc<Mutex<FnvHashMap<PeerId, KadMessenger>>>,
}

impl MessengerManager {
    fn new(swarm: SwarmControl, config: KademliaProtocolConfig) -> Self {
        Self { swarm, config, cache: Arc::new(Default::default()) }
    }

    pub(crate) async fn get_messenger(&mut self, peer: &PeerId) -> Result<KadMessenger> {
        // lock as little as possible
        let r = {
            let mut cache = self.cache.lock().await;
            cache.remove(peer)
        };

        match r {
            Some(sender) => {
                Ok(sender)
            }
            None => {
                // make a new sender
                KadMessenger::build(self.swarm.clone(), peer.clone(), self.config.clone()).await
            }
        }
    }

    pub(crate) async fn put_messenger(&mut self, mut messenger: KadMessenger) {
        if messenger.reuse().await {
            let mut cache = self.cache.lock().await;
            let peer = messenger.get_peer_id();

            // perhaps there is a messenger in the hashmap already
            if !cache.contains_key(peer) {
                cache.insert(peer.clone(), messenger);
            }
        }
    }
}