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
use libp2prs_core::{PeerId, Multiaddr};

use crate::record;
use crate::query::QueryId;

pub(crate) enum ControlCommand {
    /// Lookups the closer peers with given ID, returns a list of peer info
    /// with relevant addresses.
    Lookup(record::Key, oneshot::Sender<Option<Vec<Multiaddr>>>),
    /// Searches for a peer with given ID, returns a list of peer info
    /// with relevant addresses.
    FindPeer(PeerId, oneshot::Sender<Option<Vec<Multiaddr>>>),
    /// Lookup peers who are able to provide a given key.
    ///
    /// When count is 0, this method will return an unbounded number of
    /// results.
    FindProviders(record::Key, oneshot::Sender<Option<QueryId>>),
    /// Provide adds the given key to the content routing system.
    /// It also announces it, otherwise it is just kept in the local
    /// accounting of which objects are being provided.
    Providing(record::Key, oneshot::Sender<()>),
    /// Adds value corresponding to given Key.
    PutValue(record::Key, oneshot::Sender<()>),
    /// Searches value corresponding to given Key.
    GetValue(record::Key, oneshot::Sender<()>),
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

    /// Lookup the closer peers with the given key.
    pub async fn lookup(&mut self, key: record::Key) -> Option<Vec<Multiaddr>> {
        let (tx, rx) = oneshot::channel();
        self.control_sender
            .send(ControlCommand::Lookup(key, tx))
            .await
            .expect("control send lookup");
        rx.await.expect("lookup")
    }

    pub async fn find_peer(&mut self, peer_id: &PeerId) -> Option<Vec<Multiaddr>> {
        let (tx, rx) = oneshot::channel();
        self.control_sender
            .send(ControlCommand::FindPeer(peer_id.clone(), tx))
            .await
            .expect("control send find_peer");
        rx.await.expect("find_peer")
    }
}
