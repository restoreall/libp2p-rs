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

use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
use libp2prs_core::peerstore::{AddrBookRecord, PeerStore};
use libp2prs_core::{Multiaddr, PeerId, PublicKey};
use smallvec::SmallVec;
use std::sync::Arc;
use std::time::Duration;

use crate::connection::ConnectionId;
use crate::identify::IdentifyInfo;
use crate::metrics::metric::Metric;
use crate::network::NetworkInfo;
use crate::substream::{StreamId, Substream};
use crate::{ProtocolId, SwarmError};

type Result<T> = std::result::Result<T, SwarmError>;

/// The control commands for [`Swarm`].
///
/// The `Swarm` controller manipulates the [`Swarm`] via these commands.
///
#[derive(Debug)]
#[allow(dead_code)]
pub enum SwarmControlCmd {
    /// Open a connection to the remote peer with address specified.
    Connect(PeerId, Vec<Multiaddr>, oneshot::Sender<Result<()>>),
    /// Open a connection to the remote peer. Parameter 'bool' means using DHT(if available) to
    /// look for multiaddr of the remote peer.
    NewConnection(PeerId, bool, oneshot::Sender<Result<()>>),
    /// Close any connection to the remote peer.
    CloseConnection(PeerId, oneshot::Sender<Result<()>>),
    /// Open a new stream specified with protocol Ids to the remote peer.
    /// Parameter 'bool' means using DHT(if available) to look for
    /// multiaddr of the remote peer.
    NewStream(PeerId, Vec<ProtocolId>, bool, oneshot::Sender<Result<Substream>>),
    /// Close a stream specified.
    CloseStream(ConnectionId, StreamId),
    /// Close the whole connection.
    CloseSwarm,
    /// Retrieve network information of Swarm
    NetworkInfo(oneshot::Sender<Result<NetworkInfo>>),
    /// Retrieve network information of Swarm
    IdentifyInfo(oneshot::Sender<Result<IdentifyInfo>>),
}

/// The `Swarm` controller.
///
/// While a Yamux connection makes progress via its `next_stream` method,
/// this controller can be used to concurrently direct the connection,
/// e.g. to open a new stream to the remote or to close the connection.
///
#[derive(Clone)]
pub struct Control {
    /// Command channel to `Connection`.
    sender: mpsc::Sender<SwarmControlCmd>,
    /// PeerStore
    peer_store: PeerStore,
    /// Swarm metric
    metric: Arc<Metric>,
}

impl Control {
    pub(crate) fn new(sender: mpsc::Sender<SwarmControlCmd>, peer_store: PeerStore, metric: Arc<Metric>) -> Self {
        Control {
            sender,
            peer_store,
            metric,
        }
    }

    /// Get recv package count&bytes
    pub fn get_recv_count_and_size(&self) -> (usize, usize) {
        self.metric.get_recv_count_and_size()
    }

    /// Get send package count&bytes
    pub fn get_sent_count_and_size(&self) -> (usize, usize) {
        self.metric.get_sent_count_and_size()
    }

    /// Get recv&send bytes by protocol_id
    pub fn get_protocol_in_and_out(&self, protocol_id: &ProtocolId) -> (Option<usize>, Option<usize>) {
        self.metric.get_protocol_in_and_out(protocol_id)
    }

    /// Get recv&send bytes by peer_id
    pub fn get_peer_in_and_out(&self, peer_id: &PeerId) -> (Option<usize>, Option<usize>) {
        self.metric.get_peer_in_and_out(peer_id)
    }

    /// Make a new connection towards the remote peer with address specified.
    pub async fn connect(&mut self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<()> {
        let (tx, rx) = oneshot::channel::<Result<()>>();
        let _ = self.sender.send(SwarmControlCmd::Connect(peer_id, addrs, tx)).await;
        rx.await?
    }

    /// Make a new connection towards the remote peer.
    ///
    /// It will lookup the peer store for address of the peer,
    /// otherwise initiate Kad-DHT for address querying, when DHT is enabled.
    pub async fn new_connection(&mut self, peer_id: PeerId) -> Result<()> {
        let (tx, rx) = oneshot::channel::<Result<()>>();
        let _ = self.sender.send(SwarmControlCmd::NewConnection(peer_id, true, tx)).await;
        rx.await?
    }

    /// Make a new connection towards the remote peer, without using DHT.
    pub async fn new_connection_no_dht(&mut self, peer_id: PeerId) -> Result<()> {
        let (tx, rx) = oneshot::channel::<Result<()>>();
        let _ = self.sender.send(SwarmControlCmd::NewConnection(peer_id, false, tx)).await;
        rx.await?
    }

    /// Open a new outbound stream towards the remote peer.
    ///
    /// It will lookup the peer store for address of the peer,
    /// otherwise initiate Kad-DHT for address querying, when DHT is enabled.
    /// In the end, it will open an outgoing sub-stream when the connection is
    /// eventually established.
    pub async fn new_stream(&mut self, peer_id: PeerId, pids: Vec<ProtocolId>) -> Result<Substream> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(SwarmControlCmd::NewStream(peer_id, pids, true, tx)).await?;
        rx.await?
    }

    /// Open a new outbound stream towards the remote peer, without using DHT.
    pub async fn new_stream_no_dht(&mut self, peer_id: PeerId, pids: Vec<ProtocolId>) -> Result<Substream> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(SwarmControlCmd::NewStream(peer_id, pids, false, tx)).await?;
        rx.await?
    }

    /// Retrieve network statistics from Swarm.
    pub async fn retrieve_networkinfo(&mut self) -> Result<NetworkInfo> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(SwarmControlCmd::NetworkInfo(tx)).await?;
        rx.await?
    }

    /// Close the swarm.
    pub async fn close(&mut self) -> Result<()> {
        // SwarmControlCmd::CloseSwarm doesn't need a response from Swarm
        if self.sender.send(SwarmControlCmd::CloseSwarm).await.is_err() {
            // The receiver is closed which means the connection is already closed.
            return Ok(());
        }
        self.sender.close_channel();
        std::thread::sleep(Duration::from_secs(5));
        log::info!("Exit success");
        Ok(())
    }

    /// Insert a public key, indexed by peer_id.
    pub fn add_key(&self, peer_id: &PeerId, key: PublicKey) {
        self.peer_store.add_key(peer_id, key)
    }
    /// Delete public key by peer_id.
    pub fn del_key(&self, peer_id: &PeerId) {
        self.peer_store.del_key(peer_id);
    }

    /// Get public key by peer_id.
    pub fn get_key(&self, peer_id: &PeerId) -> Option<PublicKey> {
        self.peer_store.get_key(peer_id)
    }

    /// Get multiaddr of a peer.
    pub fn get_addr(&self, peer_id: &PeerId) -> Option<SmallVec<[AddrBookRecord; 4]>> {
        self.peer_store.get_addr(peer_id)
    }

    /// Get multiaddr of a peer.
    pub fn get_addrs_vec(&self, peer_id: &PeerId) -> Option<Vec<Multiaddr>> {
        let r = self.peer_store.get_addr(peer_id);
        r.map(|r| r.into_iter().map(|r| r.into_maddr()).collect())
    }

    /// Add a address to address_book by peer_id, if exists, update rtt.
    pub fn add_addr(&self, peer_id: &PeerId, addr: Multiaddr, ttl: Duration, is_kad: bool) {
        self.peer_store.add_addr(peer_id, addr, ttl, is_kad)
    }

    /// Add many new addresses if they're not already in the Address Book.
    pub fn add_addrs(&self, peer_id: &PeerId, addrs: Vec<Multiaddr>, ttl: Duration, is_kad: bool) {
        self.peer_store.add_addrs(peer_id, addrs, ttl, is_kad)
    }

    /// Delete all multiaddr of a peer from address book.
    pub fn clear_addrs(&self, peer_id: &PeerId) {
        self.peer_store.clear_addrs(peer_id)
    }

    /// Update ttl if current_ttl equals old_ttl.
    pub fn update_addr(&self, peer_id: &PeerId, new_ttl: Duration) {
        self.peer_store.update_addr(peer_id, new_ttl)
    }
    /// Get smallvec by peer_id and remove expired address
    pub fn remove_expired_addr(&self, peer_id: &PeerId) {
        self.peer_store.remove_expired_addr(peer_id)
    }

    /// Insert supported protocol by peer_id
    pub fn add_protocol(&self, peer_id: &PeerId, proto: Vec<String>) {
        self.peer_store.add_protocol(peer_id, proto);
    }

    /// Remove support protocol by peer_id
    pub fn remove_protocol(&self, peer_id: &PeerId) {
        self.peer_store.remove_protocol(peer_id);
    }

    pub fn get_protocol(&self, peer_id: &PeerId) -> Option<Vec<String>> {
        self.peer_store.get_protocol(peer_id)
    }
}
