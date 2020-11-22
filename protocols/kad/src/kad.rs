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
use std::collections::{HashMap, VecDeque, HashSet};
use std::num::NonZeroUsize;
use std::borrow::Borrow;
use fnv::FnvHashSet;

use futures::stream::FusedStream;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
    select,
};

use async_std::task;

use libp2prs_core::{PeerId, Multiaddr};
use libp2prs_swarm::substream::Substream;
use libp2prs_swarm::Control as SwarmControl;
use libp2prs_traits::{ReadEx, WriteEx};

use crate::protocol::{KadProtocolHandler, KadPeer, ProtocolEvent, KadRequestMsg, KadResponseMsg, KadConnectionType};
use crate::control::{Control, ControlCommand};

use crate::query::{QueryId, QueryPool, QueryConfig, Query, QueryStats};
use crate::jobs::{AddProviderJob, PutRecordJob};
use crate::kbucket::{KBucketsTable, NodeStatus};
use crate::store::RecordStore;
use crate::{record, kbucket, Addresses, Record, KadError, ProviderRecord};

type Result<T> = std::result::Result<T, KadError>;


/// `Kademlia` implements the libp2p Kademlia protocol.
pub struct Kademlia<TStore> {
    /// The Kademlia routing table.
    kbuckets: KBucketsTable<kbucket::Key<PeerId>, Addresses>,

    /// The k-bucket insertion strategy.
    kbucket_inserts: KademliaBucketInserts,

    /// The currently active (i.e. in-progress) queries.
    queries: QueryPool<QueryInner>,

    /// The currently connected peers.
    ///
    /// This is a superset of the connected peers currently in the routing table.
    connected_peers: FnvHashSet<PeerId>,

    /// Periodic job for re-publication of provider records for keys
    /// provided by the local node.
    add_provider_job: Option<AddProviderJob>,

    /// Periodic job for (re-)replication and (re-)publishing of
    /// regular (value-)records.
    put_record_job: Option<PutRecordJob>,

    /// The TTL of regular (value-)records.
    record_ttl: Option<Duration>,

    /// The TTL of provider records.
    provider_record_ttl: Option<Duration>,

    /// How long to keep connections alive when they're idle.
    connection_idle_timeout: Duration,

    /// Queued events to return when the behaviour is being polled.
    queued_events: VecDeque<NetworkBehaviourAction<KademliaHandlerIn<QueryId>, KademliaEvent>>,

    /// The currently known addresses of the local node.
    local_addrs: HashSet<Multiaddr>,

    /// The record storage.
    store: TStore,

    // Used to communicate with Swarm.
    swarm: Option<SwarmControl>,

    // New peer is connected or peer is dead.
    // peer_tx: mpsc::UnboundedSender<PeerEvent>,
    // peer_rx: mpsc::UnboundedReceiver<PeerEvent>,

    /// Used to handle the incoming Kad messages.
    incoming_tx: mpsc::UnboundedSender<ProtocolEvent<u32>>,
    incoming_rx: mpsc::UnboundedReceiver<ProtocolEvent<u32>>,

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

impl<TStore> Kademlia<TStore>
    where
            for<'a> TStore: RecordStore<'a>
{
    /// Creates a new `Kademlia` network behaviour with a default configuration.
    pub fn new(id: PeerId, store: TStore) -> Self {
        Self::with_config(id, store, Default::default())
    }

    /// Get the protocol name of this kademlia instance.
    pub fn protocol_name(&self) -> &[u8] {
        self.protocol_config.protocol_name()
    }

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

        let (incoming_tx, incoming_rx) = mpsc::unbounded();
        let (control_tx, control_rx) = mpsc::unbounded();

        Kademlia {
            store,
            swarm: None,
            incoming_rx,
            incoming_tx,
            control_tx,
            control_rx,
            kbuckets: KBucketsTable::new(local_key, config.kbucket_pending_timeout),
            kbucket_inserts: config.kbucket_inserts,
            queued_events: VecDeque::with_capacity(config.query_config.replication_factor.get()),
            queries: QueryPool::new(config.query_config),
            connected_peers: Default::default(),
            add_provider_job,
            put_record_job,
            record_ttl: config.record_ttl,
            provider_record_ttl: config.provider_record_ttl,
            connection_idle_timeout: config.connection_idle_timeout,
            local_addrs: HashSet::new(),
        }
    }

    /// Gets an iterator over immutable references to all running queries.
    pub fn iter_queries<'a>(&'a self) -> impl Iterator<Item = QueryRef<'a>> {
        self.queries.iter().filter_map(|query|
            if !query.is_finished() {
                Some(QueryRef { query })
            } else {
                None
            })
    }

    /// Gets an iterator over mutable references to all running queries.
    pub fn iter_queries_mut<'a>(&'a mut self) -> impl Iterator<Item = QueryMut<'a>> {
        self.queries.iter_mut().filter_map(|query|
            if !query.is_finished() {
                Some(QueryMut { query })
            } else {
                None
            })
    }

    /// Gets an immutable reference to a running query, if it exists.
    pub fn query<'a>(&'a self, id: &QueryId) -> Option<QueryRef<'a>> {
        self.queries.get(id).and_then(|query|
            if !query.is_finished() {
                Some(QueryRef { query })
            } else {
                None
            })
    }

    /// Gets a mutable reference to a running query, if it exists.
    pub fn query_mut<'a>(&'a mut self, id: &QueryId) -> Option<QueryMut<'a>> {
        self.queries.get_mut(id).and_then(|query|
            if !query.is_finished() {
                Some(QueryMut { query })
            } else {
                None
            })
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
    pub fn kbuckets(&mut self)
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

    /// Initiates an iterative query for the closest peers to the given key.
    ///
    /// The result of the query is delivered in a
    /// [`KademliaEvent::QueryResult{QueryResult::GetClosestPeers}`].
    pub fn get_closest_peers<K>(&mut self, key: K) -> QueryId
        where
            K: Borrow<[u8]> + Clone
    {
        let info = QueryInfo::GetClosestPeers { key: key.borrow().to_vec() };
        let target = kbucket::Key::new(key);
        let peers = self.kbuckets.closest_keys(&target);
        let inner = QueryInner::new(info);
        self.queries.add_iter_closest(target.clone(), peers, inner)
    }

    /// Performs a lookup for a record in the DHT.
    ///
    /// The result of this operation is delivered in a
    /// [`KademliaEvent::QueryResult{QueryResult::GetRecord}`].
    pub fn get_record(&mut self, key: &record::Key, quorum: Quorum) -> QueryId {
        let quorum = quorum.eval(self.queries.config().replication_factor);
        let mut records = Vec::with_capacity(quorum.get());

        if let Some(record) = self.store.get(key) {
            if record.is_expired(Instant::now()) {
                self.store.remove(key)
            } else {
                records.push(PeerRecord{ peer: None, record: record.into_owned()});
            }
        }

        let done = records.len() >= quorum.get();
        let target = kbucket::Key::new(key.clone());
        let info = QueryInfo::GetRecord { key: key.clone(), records, quorum, cache_at: None };
        let peers = self.kbuckets.closest_keys(&target);
        let inner = QueryInner::new(info);
        let id = self.queries.add_iter_closest(target.clone(), peers, inner); // (*)

        // Instantly finish the query if we already have enough records.
        if done {
            self.queries.get_mut(&id).expect("by (*)").finish();
        }

        id
    }

    /// Stores a record in the DHT.
    ///
    /// Returns `Ok` if a record has been stored locally, providing the
    /// `QueryId` of the initial query that replicates the record in the DHT.
    /// The result of the query is eventually reported as a
    /// [`KademliaEvent::QueryResult{QueryResult::PutRecord}`].
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
    pub fn put_record(&mut self, mut record: Record, quorum: Quorum) -> Result<QueryId> {
        record.publisher = Some(self.kbuckets.local_key().preimage().clone());
        self.store.put(record.clone())?;
        record.expires = record.expires.or_else(||
            self.record_ttl.map(|ttl| Instant::now() + ttl));
        let quorum = quorum.eval(self.queries.config().replication_factor);
        let target = kbucket::Key::new(record.key.clone());
        let peers = self.kbuckets.closest_keys(&target);
        let context = PutRecordContext::Publish;
        let info = QueryInfo::PutRecord {
            context,
            record,
            quorum,
            phase: PutRecordPhase::GetClosestPeers
        };
        let inner = QueryInner::new(info);
        Ok(self.queries.add_iter_closest(target.clone(), peers, inner))
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
    pub fn remove_record(&mut self, key: &record::Key) {
        if let Some(r) = self.store.get(key) {
            if r.publisher.as_ref() == Some(self.kbuckets.local_key().preimage()) {
                self.store.remove(key)
            }
        }
    }

    /// Gets a mutable reference to the record store.
    pub fn store_mut(&mut self) -> &mut TStore {
        &mut self.store
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
    pub fn bootstrap(&mut self) -> Result<QueryId> {
        let local_key = self.kbuckets.local_key().clone();
        let info = QueryInfo::Bootstrap {
            peer: local_key.preimage().clone(),
            remaining: None
        };
        let peers = self.kbuckets.closest_keys(&local_key).collect::<Vec<_>>();
        if peers.is_empty() {
            Err(KadError::NoKnownPeers)
        } else {
            let inner = QueryInner::new(info);
            Ok(self.queries.add_iter_closest(local_key, peers, inner))
        }
    }

    /// Establishes the local node as a provider of a value for the given key.
    ///
    /// This operation publishes a provider record with the given key and
    /// identity of the local node to the peers closest to the key, thus establishing
    /// the local node as a provider.
    ///
    /// Returns `Ok` if a provider record has been stored locally, providing the
    /// `QueryId` of the initial query that announces the local node as a provider.
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
    /// The results of the (repeated) provider announcements sent by this node are
    /// reported via [`KademliaEvent::QueryResult{QueryResult::StartProviding}`].
    pub fn start_providing(&mut self, key: record::Key) -> Result<QueryId> {
        // Note: We store our own provider records locally without local addresses
        // to avoid redundant storage and outdated addresses. Instead these are
        // acquired on demand when returning a `ProviderRecord` for the local node.
        let local_addrs = Vec::new();
        let record = ProviderRecord::new(
            key.clone(),
            self.kbuckets.local_key().preimage().clone(),
            local_addrs);
        self.store.add_provider(record)?;
        let target = kbucket::Key::new(key.clone());
        let peers = self.kbuckets.closest_keys(&target);
        let context = AddProviderContext::Publish;
        let info = QueryInfo::AddProvider {
            context,
            key,
            phase: AddProviderPhase::GetClosestPeers
        };
        let inner = QueryInner::new(info);
        let id = self.queries.add_iter_closest(target.clone(), peers, inner);
        Ok(id)
    }

    /// Stops the local node from announcing that it is a provider for the given key.
    ///
    /// This is a local operation. The local node will still be considered as a
    /// provider for the key by other nodes until these provider records expire.
    pub fn stop_providing(&mut self, key: &record::Key) {
        self.store.remove_provider(key, self.kbuckets.local_key().preimage());
    }

    /// Performs a lookup for providers of a value to the given key.
    ///
    /// The result of this operation is delivered in a
    /// reported via [`KademliaEvent::QueryResult{QueryResult::GetProviders}`].
    pub fn get_providers(&mut self, key: record::Key) -> QueryId {
        let info = QueryInfo::GetProviders {
            key: key.clone(),
            providers: HashSet::new(),
        };
        let target = kbucket::Key::new(key);
        let peers = self.kbuckets.closest_keys(&target);
        let inner = QueryInner::new(info);
        self.queries.add_iter_closest(target.clone(), peers, inner)
    }

    /// Processes discovered peers from a successful request in an iterative `Query`.
    fn discovered<'a, I>(&'a mut self, query_id: &QueryId, source: &PeerId, peers: I)
        where
            I: Iterator<Item = &'a KadPeer> + Clone
    {
        let local_id = self.kbuckets.local_key().preimage().clone();
        let others_iter = peers.filter(|p| p.node_id != local_id);
        if let Some(query) = self.queries.get_mut(query_id) {
            log::trace!("Request to {:?} in query {:?} succeeded.", source, query_id);
            for peer in others_iter.clone() {
                log::trace!("Peer {:?} reported by {:?} in query {:?}.",
                            peer, source, query_id);
                let addrs = peer.multiaddrs.iter().cloned().collect();
                query.inner.addresses.insert(peer.node_id.clone(), addrs);
            }
            query.on_success(source, others_iter.cloned().map(|kp| kp.node_id))
        }
    }

    /// Finds the closest peers to a `target` in the context of a request by
    /// the `source` peer, such that the `source` peer is never included in the
    /// result.
    fn find_closest<T: Clone>(&mut self, target: &kbucket::Key<T>, source: &PeerId) -> Vec<KadPeer> {
        if target == self.kbuckets.local_key() {
            Vec::new()
        } else {
            self.kbuckets
                .closest(target)
                .filter(|e| e.node.key.preimage() != source)
                .take(self.queries.config().replication_factor.get())
                .map(KadPeer::from)
                .collect()
        }
    }

    /// Collects all peers who are known to be providers of the value for a given `Multihash`.
    fn provider_peers(&mut self, key: &record::Key, source: &PeerId) -> Vec<KadPeer> {
        let kbuckets = &mut self.kbuckets;
        let connected = &mut self.connected_peers;
        let local_addrs = &self.local_addrs;
        self.store.providers(key)
            .into_iter()
            .filter_map(move |p|
                if &p.provider != source {
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
                        if &node_id == kbuckets.local_key().preimage() {
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
            .take(self.queries.config().replication_factor.get())
            .collect()
    }

    /// Starts an iterative `ADD_PROVIDER` query for the given key.
    fn start_add_provider(&mut self, key: record::Key, context: AddProviderContext) {
        let info = QueryInfo::AddProvider {
            context,
            key: key.clone(),
            phase: AddProviderPhase::GetClosestPeers
        };
        let target = kbucket::Key::new(key);
        let peers = self.kbuckets.closest_keys(&target);
        let inner = QueryInner::new(info);
        self.queries.add_iter_closest(target.clone(), peers, inner);
    }

    /// Starts an iterative `PUT_VALUE` query for the given record.
    fn start_put_record(&mut self, record: Record, quorum: Quorum, context: PutRecordContext) {
        let quorum = quorum.eval(self.queries.config().replication_factor);
        let target = kbucket::Key::new(record.key.clone());
        let peers = self.kbuckets.closest_keys(&target);
        let info = QueryInfo::PutRecord {
            record, quorum, context, phase: PutRecordPhase::GetClosestPeers
        };
        let inner = QueryInner::new(info);
        self.queries.add_iter_closest(target.clone(), peers, inner);
    }

    /// Updates the routing table with a new connection status and address of a peer.
    fn connection_updated(&mut self, peer: PeerId, address: Option<Multiaddr>, new_status: NodeStatus) {
        let key = kbucket::Key::new(peer.clone());
        match self.kbuckets.entry(&key) {
            kbucket::Entry::Present(mut entry, old_status) => {
                if let Some(address) = address {
                    if entry.value().insert(address) {
                        self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                            KademliaEvent::RoutingUpdated {
                                peer,
                                addresses: entry.value().clone(),
                                old_peer: None,
                            }
                        ))
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
                        self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                            KademliaEvent::UnroutablePeer { peer }
                        ));
                    }
                    (Some(a), KademliaBucketInserts::Manual) => {
                        self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                            KademliaEvent::RoutablePeer { peer, address: a }
                        ));
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
                                self.queued_events.push_back(
                                    NetworkBehaviourAction::GenerateEvent(event));
                            },
                            kbucket::InsertResult::Full => {
                                log::debug!("Bucket full. Peer not added to routing table: {}", peer);
                                let address = addresses.first().clone();
                                self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                                    KademliaEvent::RoutablePeer { peer, address }
                                ));
                            },
                            kbucket::InsertResult::Pending { disconnected } => {
                                debug_assert!(!self.connected_peers.contains(disconnected.preimage()));
                                let address = addresses.first().clone();
                                self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                                    KademliaEvent::PendingRoutablePeer { peer, address }
                                ));
                                self.queued_events.push_back(NetworkBehaviourAction::DialPeer {
                                    peer_id: disconnected.into_preimage(),
                                    condition: DialPeerCondition::Disconnected
                                })
                            },
                        }
                    }
                }
            },
            _ => {}
        }
    }

    /// Handles a finished (i.e. successful) query.
    fn query_finished(&mut self, q: Query<QueryInner>, params: &mut impl PollParameters)
                      -> Option<KademliaEvent>
    {
        let query_id = q.id();
        log::trace!("Query {:?} finished.", query_id);
        let result = q.into_result();
        match result.inner.info {
            QueryInfo::Bootstrap { peer, remaining } => {
                let local_key = self.kbuckets.local_key().clone();
                let mut remaining = remaining.unwrap_or_else(|| {
                    debug_assert_eq!(&peer, local_key.preimage());
                    // The lookup for the local key finished. To complete the bootstrap process,
                    // a bucket refresh should be performed for every bucket farther away than
                    // the first non-empty bucket (which are most likely no more than the last
                    // few, i.e. farthest, buckets).
                    self.kbuckets.iter()
                        .skip_while(|b| b.is_empty())
                        .skip(1) // Skip the bucket with the closest neighbour.
                        .map(|b| {
                            // Try to find a key that falls into the bucket. While such keys can
                            // be generated fully deterministically, the current libp2p kademlia
                            // wire protocol requires transmission of the preimages of the actual
                            // keys in the DHT keyspace, hence for now this is just a "best effort"
                            // to find a key that hashes into a specific bucket. The probabilities
                            // of finding a key in the bucket `b` with as most 16 trials are as
                            // follows:
                            //
                            // Pr(bucket-255) = 1 - (1/2)^16   ~= 1
                            // Pr(bucket-254) = 1 - (3/4)^16   ~= 1
                            // Pr(bucket-253) = 1 - (7/8)^16   ~= 0.88
                            // Pr(bucket-252) = 1 - (15/16)^16 ~= 0.64
                            // ...
                            let mut target = kbucket::Key::new(PeerId::random());
                            for _ in 0 .. 16 {
                                let d = local_key.distance(&target);
                                if b.contains(&d) {
                                    break;
                                }
                                target = kbucket::Key::new(PeerId::random());
                            }
                            target
                        }).collect::<Vec<_>>().into_iter()
                });

                let num_remaining = remaining.len().saturating_sub(1) as u32;

                if let Some(target) = remaining.next() {
                    let info = QueryInfo::Bootstrap {
                        peer: target.clone().into_preimage(),
                        remaining: Some(remaining)
                    };
                    let peers = self.kbuckets.closest_keys(&target);
                    let inner = QueryInner::new(info);
                    self.queries.continue_iter_closest(query_id, target.clone(), peers, inner);
                }

                Some(KademliaEvent::QueryResult {
                    id: query_id,
                    stats: result.stats,
                    result: QueryResult::Bootstrap(Ok(BootstrapOk { peer, num_remaining }))
                })
            }

            QueryInfo::GetClosestPeers { key, .. } => {
                Some(KademliaEvent::QueryResult {
                    id: query_id,
                    stats: result.stats,
                    result: QueryResult::GetClosestPeers(Ok(
                        GetClosestPeersOk { key, peers: result.peers.collect() }
                    ))
                })
            }

            QueryInfo::GetProviders { key, providers } => {
                Some(KademliaEvent::QueryResult {
                    id: query_id,
                    stats: result.stats,
                    result: QueryResult::GetProviders(Ok(
                        GetProvidersOk {
                            key,
                            providers,
                            closest_peers: result.peers.collect()
                        }
                    ))
                })
            }

            QueryInfo::AddProvider {
                context,
                key,
                phase: AddProviderPhase::GetClosestPeers
            } => {
                let provider_id = params.local_peer_id().clone();
                let external_addresses = params.external_addresses().collect();
                let inner = QueryInner::new(QueryInfo::AddProvider {
                    context,
                    key,
                    phase: AddProviderPhase::AddProvider {
                        provider_id,
                        external_addresses,
                        get_closest_peers_stats: result.stats
                    }
                });
                self.queries.continue_fixed(query_id, result.peers, inner);
                None
            }

            QueryInfo::AddProvider {
                context,
                key,
                phase: AddProviderPhase::AddProvider { get_closest_peers_stats, .. }
            } => {
                match context {
                    AddProviderContext::Publish => {
                        Some(KademliaEvent::QueryResult {
                            id: query_id,
                            stats: get_closest_peers_stats.merge(result.stats),
                            result: QueryResult::StartProviding(Ok(AddProviderOk { key }))
                        })
                    }
                    AddProviderContext::Republish => {
                        Some(KademliaEvent::QueryResult {
                            id: query_id,
                            stats: get_closest_peers_stats.merge(result.stats),
                            result: QueryResult::RepublishProvider(Ok(AddProviderOk { key }))
                        })
                    }
                }
            }

            QueryInfo::GetRecord { key, records, quorum, cache_at } => {
                let results = if records.len() >= quorum.get() { // [not empty]
                    if let Some(cache_key) = cache_at {
                        // Cache the record at the closest node to the key that
                        // did not return the record.
                        let record = records.first().expect("[not empty]").record.clone();
                        let quorum = NonZeroUsize::new(1).expect("1 > 0");
                        let context = PutRecordContext::Cache;
                        let info = QueryInfo::PutRecord {
                            context,
                            record,
                            quorum,
                            phase: PutRecordPhase::PutRecord {
                                success: vec![],
                                get_closest_peers_stats: QueryStats::empty()
                            }
                        };
                        let inner = QueryInner::new(info);
                        self.queries.add_fixed(iter::once(cache_key.into_preimage()), inner);
                    }
                    Ok(GetRecordOk { records })
                } else if records.is_empty() {
                    Err(GetRecordError::NotFound {
                        key,
                        closest_peers: result.peers.collect()
                    })
                } else {
                    Err(GetRecordError::QuorumFailed { key, records, quorum })
                };
                Some(KademliaEvent::QueryResult {
                    id: query_id,
                    stats: result.stats,
                    result: QueryResult::GetRecord(results)
                })
            }

            QueryInfo::PutRecord {
                context,
                record,
                quorum,
                phase: PutRecordPhase::GetClosestPeers
            } => {
                let info = QueryInfo::PutRecord {
                    context,
                    record,
                    quorum,
                    phase: PutRecordPhase::PutRecord {
                        success: vec![],
                        get_closest_peers_stats: result.stats
                    }
                };
                let inner = QueryInner::new(info);
                self.queries.continue_fixed(query_id, result.peers, inner);
                None
            }

            QueryInfo::PutRecord {
                context,
                record,
                quorum,
                phase: PutRecordPhase::PutRecord { success, get_closest_peers_stats }
            } => {
                let mk_result = |key: record::Key| {
                    if success.len() >= quorum.get() {
                        Ok(PutRecordOk { key })
                    } else {
                        Err(PutRecordError::QuorumFailed { key, quorum, success })
                    }
                };
                match context {
                    PutRecordContext::Publish =>
                        Some(KademliaEvent::QueryResult {
                            id: query_id,
                            stats: get_closest_peers_stats.merge(result.stats),
                            result: QueryResult::PutRecord(mk_result(record.key))
                        }),
                    PutRecordContext::Republish =>
                        Some(KademliaEvent::QueryResult {
                            id: query_id,
                            stats: get_closest_peers_stats.merge(result.stats),
                            result: QueryResult::RepublishRecord(mk_result(record.key))
                        }),
                    PutRecordContext::Replicate => {
                        log::debug!("Record replicated: {:?}", record.key);
                        None
                    }
                    PutRecordContext::Cache => {
                        log::debug!("Record cached: {:?}", record.key);
                        None
                    }
                }
            }
        }
    }

    /// Handles a query that timed out.
    fn query_timeout(&mut self, query: Query<QueryInner>) -> Option<KademliaEvent> {
        let query_id = query.id();
        log::trace!("Query {:?} timed out.", query_id);
        let result = query.into_result();
        match result.inner.info {
            QueryInfo::Bootstrap { peer, mut remaining } => {
                let num_remaining = remaining.as_ref().map(|r| r.len().saturating_sub(1) as u32);

                if let Some(mut remaining) = remaining.take() {
                    // Continue with the next bootstrap query if `remaining` is not empty.
                    if let Some(target) = remaining.next() {
                        let info = QueryInfo::Bootstrap {
                            peer: target.clone().into_preimage(),
                            remaining: Some(remaining)
                        };
                        let peers = self.kbuckets.closest_keys(&target);
                        let inner = QueryInner::new(info);
                        self.queries.continue_iter_closest(query_id, target.clone(), peers, inner);
                    }
                }

                Some(KademliaEvent::QueryResult {
                    id: query_id,
                    stats: result.stats,
                    result: QueryResult::Bootstrap(Err(
                        BootstrapError::Timeout { peer, num_remaining }
                    ))
                })
            }

            QueryInfo::AddProvider { context, key, .. } =>
                Some(match context {
                    AddProviderContext::Publish =>
                        KademliaEvent::QueryResult {
                            id: query_id,
                            stats: result.stats,
                            result: QueryResult::StartProviding(Err(
                                AddProviderError::Timeout { key }
                            ))
                        },
                    AddProviderContext::Republish =>
                        KademliaEvent::QueryResult {
                            id: query_id,
                            stats: result.stats,
                            result: QueryResult::RepublishProvider(Err(
                                AddProviderError::Timeout { key }
                            ))
                        }
                }),

            QueryInfo::GetClosestPeers { key } => {
                Some(KademliaEvent::QueryResult {
                    id: query_id,
                    stats: result.stats,
                    result: QueryResult::GetClosestPeers(Err(
                        GetClosestPeersError::Timeout {
                            key,
                            peers: result.peers.collect()
                        }
                    ))
                })
            },

            QueryInfo::PutRecord { record, quorum, context, phase } => {
                let err = Err(PutRecordError::Timeout {
                    key: record.key,
                    quorum,
                    success: match phase {
                        PutRecordPhase::GetClosestPeers => vec![],
                        PutRecordPhase::PutRecord { ref success, .. } => success.clone(),
                    }
                });
                match context {
                    PutRecordContext::Publish =>
                        Some(KademliaEvent::QueryResult {
                            id: query_id,
                            stats: result.stats,
                            result: QueryResult::PutRecord(err)
                        }),
                    PutRecordContext::Republish =>
                        Some(KademliaEvent::QueryResult {
                            id: query_id,
                            stats: result.stats,
                            result: QueryResult::RepublishRecord(err)
                        }),
                    PutRecordContext::Replicate => match phase {
                        PutRecordPhase::GetClosestPeers => {
                            log::warn!("Locating closest peers for replication failed: {:?}", err);
                            None
                        }
                        PutRecordPhase::PutRecord { .. } => {
                            log::debug!("Replicating record failed: {:?}", err);
                            None
                        }
                    }
                    PutRecordContext::Cache => match phase {
                        PutRecordPhase::GetClosestPeers => {
                            // Caching a record at the closest peer to a key that did not return
                            // a record is never preceded by a lookup for the closest peers, i.e.
                            // it is a direct query to a single peer.
                            unreachable!()
                        }
                        PutRecordPhase::PutRecord { .. } => {
                            log::debug!("Caching record failed: {:?}", err);
                            None
                        }
                    }
                }
            }

            QueryInfo::GetRecord { key, records, quorum, .. } =>
                Some(KademliaEvent::QueryResult {
                    id: query_id,
                    stats: result.stats,
                    result: QueryResult::GetRecord(Err(
                        GetRecordError::Timeout { key, records, quorum },
                    ))
                }),

            QueryInfo::GetProviders { key, providers } =>
                Some(KademliaEvent::QueryResult {
                    id: query_id,
                    stats: result.stats,
                    result: QueryResult::GetProviders(Err(
                        GetProvidersError::Timeout {
                            key,
                            providers,
                            closest_peers: result.peers.collect()
                        }
                    ))
                })
        }
    }

    /// Processes a record received from a peer.
    fn record_received(
        &mut self,
        source: PeerId,
        connection: ConnectionId,
        request_id: KademliaRequestId,
        mut record: Record
    ) {
        if record.publisher.as_ref() == Some(self.kbuckets.local_key().preimage()) {
            // If the (alleged) publisher is the local node, do nothing. The record of
            // the original publisher should never change as a result of replication
            // and the publisher is always assumed to have the "right" value.
            self.queued_events.push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id: source,
                handler: NotifyHandler::One(connection),
                event: KademliaHandlerIn::PutRecordRes {
                    key: record.key,
                    value: record.value,
                    request_id,
                },
            });
            return
        }

        let now = Instant::now();

        // Calculate the expiration exponentially inversely proportional to the
        // number of nodes between the local node and the closest node to the key
        // (beyond the replication factor). This ensures avoiding over-caching
        // outside of the k closest nodes to a key.
        let target = kbucket::Key::new(record.key.clone());
        let num_between = self.kbuckets.count_nodes_between(&target);
        let k = self.queries.config().replication_factor.get();
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
                    self.queued_events.push_back(NetworkBehaviourAction::NotifyHandler {
                        peer_id: source,
                        handler: NotifyHandler::One(connection),
                        event: KademliaHandlerIn::Reset(request_id)
                    });

                    return
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
        self.queued_events.push_back(NetworkBehaviourAction::NotifyHandler {
            peer_id: source,
            handler: NotifyHandler::One(connection),
            event: KademliaHandlerIn::PutRecordRes {
                key: record.key,
                value: record.value,
                request_id,
            },
        })
    }

    /// Processes a provider record received from a peer.
    fn provider_received(&mut self, key: record::Key, provider: KadPeer) {
        if &provider.node_id != self.kbuckets.local_key().preimage() {
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
        KadProtocolHandler::new(self.incoming_tx.clone())
    }
    /// Get the controller of Kademlia, which can be used to manipulate the Kad-DHT.
    pub fn control(&self) -> Control {
        Control::new(self.control_tx.clone())
    }

    /// Start the main message loop of Kademlia.
    pub fn start(mut self, swarm: SwarmControl) {
        self.swarm = Some(swarm);

        // well, self 'move' explicitly,
        let mut kad = self;
        task::spawn(async move {
            let _ = kad.process_loop().await;
        });
    }


    /// Message Process Loop.
    pub async fn process_loop(&mut self) -> Result<()> {
        let result = self.next().await;
        //
        // if !self.peer_rx.is_terminated() {
        //     self.peer_rx.close();
        //     while self.peer_rx.next().await.is_some() {
        //         // just drain
        //     }
        // }
        //
        // if !self.incoming_rx.is_terminated() {
        //     self.incoming_rx.close();
        //     while self.incoming_rx.next().await.is_some() {
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
                evt = self.incoming_rx.next() => {
                    self.handle_events(evt).await?;
                }
                cmd = self.control_rx.next() => {
                    self.on_control_command(cmd).await?;
                }
            }
        }
    }

    // Always wait to send message.
    async fn handle_sending_message(&mut self, rpid: PeerId, mut writer: Substream) {
        // let (mut tx, mut rx) = mpsc::unbounded();
        //
        // let _ = tx.send(self.get_hello_packet()).await;
        //
        // self.peers.insert(rpid.clone(), tx);
        //
        // task::spawn(async move {
        //     loop {
        //         match rx.next().await {
        //             Some(rpc) => {
        //                 log::trace!("send rpc msg: {:?}", rpc);
        //                 // if failed, should reset?
        //                 let _ = writer.write_one(rpc.into_bytes().as_slice()).await;
        //             }
        //             None => return,
        //         }
        //     }
        // });
    }

    // If remote peer is dead, remove it from peers and topics.
    async fn handle_remove_dead_peer(&mut self, peer_id: PeerId) {
        // let tx = self.peers.remove(&rpid);
        // match tx {
        //     Some(mut tx) => {
        //         let _ = tx.close().await;
        //     }
        //     None => return,
        // }
        //
        // for ps in self.topics.values_mut() {
        //     ps.remove(&rpid);
        // }
    }

    // Check if stream / connection is closed.
    async fn handle_peer_eof(&mut self, rpid: PeerId, mut reader: Substream) {
        // let mut peer_dead_tx = self.peer_tx.clone();
        // task::spawn(async move {
        //     loop {
        //         if reader.read_one(2048).await.is_err() {
        //             let _ = peer_dead_tx.send(PeerEvent::DeadPeer(rpid.clone())).await;
        //             return;
        //         }
        //     }
        // });
    }

    // Handle when new peer connect.
    async fn handle_add_new_peer(&mut self, pid: PeerId) {
        let stream = self.control.as_mut().unwrap().new_stream(pid.clone(), vec!["KAD_ID"]).await;

        // if new stream failed, ignore it Since some peer don's support floodsub protocol
        // if let Ok(stream) = stream {
        //     let writer = stream.clone();
        //
        //     log::trace!("open stream to {}", pid);
        //
        //     self.handle_sending_message(pid.clone(), writer).await;
        //     self.handle_peer_eof(pid.clone(), stream).await;
        // }
    }

    // Handle Kad events sent from protocol handler.
    async fn handle_events(&mut self, msg: Option<ProtocolEvent<u32>>) -> Result<()> {
        match msg {
            Some(ProtocolEvent::Connected(peer_id)) => {
                //self.handle_add_new_peer(peer_id).await;
                Ok(())
            }
            Some(ProtocolEvent::Disconnected(peer_id)) => {
                //self.handle_remove_dead_peer(peer_id).await;
                Ok(())
            }
            Some(ProtocolEvent::KadRequest {
                request,
                source,
                reply
            }) => {
                let _= handle_kad_request(request, source, reply).await?;
            }
            Some(ProtocolEvent::FindNodeReq { key, request_id }) => {
                Ok(())
            }
            Some(_) => {
                Ok(())
            }
            None => Err(KadError::Closed(1)),
        }
    }

    // Handles Kad request messages. ProtoBuf message decoded by handler.
    async fn handle_kad_request(&mut self, request: KadRequestMsg, source: PeerId, reply: oneshot::Sender<KadResponseMsg>) -> Result<()> {
        let response = match request {
            KadRequestMsg::Ping => {
                // TODO: implement; although in practice the PING message is never
                //       used, so we may consider removing it altogether
                Err(KadError("the PING Kademlia message is not implemented"))
            }
            KadRequestMsg::FindNode { key } => {
                let closer_peers = self.find_closest(&kbucket::Key::new(key), &source);
                KadResponseMsg::FindNode {
                    closer_peers
                }
            }
            KadRequestMsg::AddProvider { key, provider } => {
                // Only accept a provider record from a legitimate peer.
                if provider.node_id != source {
                    return
                }
                // TODO
            }
            KadRequestMsg::GetProviders { key } => {
                let provider_peers = self.provider_peers(&key, &source);
                let closer_peers = self.find_closest(&kbucket::Key::new(key), &source);
                KadResponseMsg::GetProviders {
                        closer_peers,
                        provider_peers,
                }

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

                KadResponseMsg::GetValue {
                        record,
                        closer_peers,
                }
            }
            KadRequestMsg::PutValue { record } => {
                self.record_received(source, connection, request_id, record);
            }
        };

        reply.send(response).await
    }

    // Process publish or subscribe command.
    async fn on_control_command(&mut self, cmd: Option<ControlCommand>) -> Result<()> {
        match cmd {
            Some(ControlCommand::Providing(key, reply)) => {
                let _ = reply.send(());
            }
            Some(ControlCommand::GetProviders(key, reply)) => {
                let _ = reply.send(None);
            }
            Some(ControlCommand::FindPeer(peer_id, reply)) => {
                let _ = reply.send(None);
            }
            Some(ControlCommand::PutValue(key, reply)) => {
                let _ = reply.send(());
            }
            Some(ControlCommand::GetValue(key, reply)) => {
                let _ = reply.send(());
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


/*
pub struct Kad {
    //config: FloodsubConfig,

    // Used to open stream.
    control: Option<SwarmControl>,

    // New peer is connected or peer is dead.
    peer_tx: mpsc::UnboundedSender<PeerEvent>,
    peer_rx: mpsc::UnboundedReceiver<PeerEvent>,

    // Used to recv incoming rpc message.
    incoming_tx: mpsc::UnboundedSender<RPC>,
    incoming_rx: mpsc::UnboundedReceiver<RPC>,

    // Used to pub/sub/ls/peers.
    control_tx: mpsc::UnboundedSender<ControlCommand>,
    control_rx: mpsc::UnboundedReceiver<ControlCommand>,

    // Connected peer.
    peers: HashMap<PeerId, mpsc::UnboundedSender<FloodsubRpc>>,
}
*/


/*
impl FloodSub {
    fn drop_all_peers(&mut self) {
        for (p, tx) in self.peers.drain().take(1) {
            tx.close_channel();
            log::trace!("drop peer {}", p);
        }
    }

    fn drop_all_my_topics(&mut self) {
        for (t, mut subs) in self.my_topics.drain().take(1) {
            for (id, tx) in subs.drain().take(1) {
                tx.close_channel();
                log::trace!("drop peer {} in myTopic {:?}", id, t.clone());
            }
        }
    }

    fn drop_all_topics(&mut self) {
        for (t, mut subs) in self.topics.drain().take(1) {
            for (p, _) in subs.drain().take(1) {
                log::trace!("drop peer {} in topic {:?}", p, t.clone());
            }
        }
    }
}
*/