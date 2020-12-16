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

use futures::channel::{mpsc, oneshot};
use futures::SinkExt;
use libp2prs_core::{Multiaddr, PeerId};

use crate::protocol::KadPeer;
use crate::query::PeerRecord;
use crate::{record, KadError};

type Result<T> = std::result::Result<T, KadError>;

pub(crate) enum ControlCommand {
    /// Initiate bootstrapping to join the Kad DHT network.
    Bootstrap,
    /// Adds a peer to Kad KBuckets, and its multiaddr to Peerstore.
    AddNode(PeerId, Vec<Multiaddr>),
    /// Removes a peer from Kad KBuckets, also removes it from Peerstore.
    RemoveNode(PeerId),
    /// Lookups the closer peers with given ID, returns a list of peer Id.
    Lookup(record::Key, oneshot::Sender<Result<Vec<KadPeer>>>),
    /// Searches for a peer with given ID, returns a list of peer info
    /// with relevant addresses.
    FindPeer(PeerId, oneshot::Sender<Result<KadPeer>>),
    /// Lookup peers who are able to provide a given key.
    ///
    FindProviders(record::Key, usize, oneshot::Sender<Result<Vec<KadPeer>>>),
    /// Provide adds the given key to the content routing system.
    /// It also announces it, otherwise it is just kept in the local
    /// accounting of which objects are being provided.
    Providing(record::Key, oneshot::Sender<Result<()>>),
    /// Adds value corresponding to given Key.
    PutValue(record::Key, Vec<u8>, oneshot::Sender<Result<()>>),
    /// Searches value corresponding to given Key.
    GetValue(record::Key, oneshot::Sender<Result<Vec<PeerRecord>>>),
}

#[derive(Clone)]
pub struct Control {
    //config: FloodsubConfig,
    control_sender: mpsc::UnboundedSender<ControlCommand>,
}

impl Control {
    pub(crate) fn new(control_sender: mpsc::UnboundedSender<ControlCommand>) -> Self {
        Control { control_sender }
    }

    /// Add a node and its listening addresses to KBuckets.
    pub async fn add_node(&mut self, peer_id: PeerId, addrs: Vec<Multiaddr>) {
        self.control_sender
            .send(ControlCommand::AddNode(peer_id, addrs))
            .await
            .expect("control send add_node");
    }

    /// Initiate bootstrapping.
    ///
    /// In general it should be done only once upon Kad startup.
    pub async fn bootstrap(&mut self) {
        self.control_sender
            .send(ControlCommand::Bootstrap)
            .await
            .expect("control send bootstrap");
    }

    /// Lookup the closer peers with the given key.
    pub async fn lookup(&mut self, key: record::Key) -> Result<Vec<KadPeer>> {
        let (tx, rx) = oneshot::channel();
        self.control_sender
            .send(ControlCommand::Lookup(key, tx))
            .await
            .expect("control send lookup");
        rx.await.expect("lookup")
    }

    pub async fn find_peer(&mut self, peer_id: &PeerId) -> Result<KadPeer> {
        let (tx, rx) = oneshot::channel();
        self.control_sender
            .send(ControlCommand::FindPeer(peer_id.clone(), tx))
            .await
            .expect("control send find_peer");
        rx.await.expect("find_peer")
    }
}
