// Copyright 2019 Parity Technologies (UK) Ltd.
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

use std::fmt;
use smallvec::SmallVec;
use libp2prs_core::Multiaddr;
use std::time::Instant;

/// The information of a peer in Kad routing table.
#[derive(Clone, Debug)]
pub struct PeerInfo {
    /// LastUsefulAt is the time instant at which the peer was last "useful" to us.
    last_used_at: Instant,

    /// The time instant at which we last got a successful query response from the peer.
    last_query_at: Instant,

    /// The time this peer was added to the routing table.
    added_at: Instant,

    /// If a bucket is full, this peer can be replaced to make space for a new peer.
    replaceable: bool
}

impl PeerInfo {
    pub(crate) fn new() -> Self {
        Self {
            last_used_at: Instant::now(),
            last_query_at: Instant::now(),
            added_at: Instant::now(),
            replaceable: false
        }
    }

    pub(crate) fn set_last_query_at(mut self, last_query: Instant) {
        self.last_query_at = last_query;
    }

    pub(crate) fn set_last_used_at(&mut self, last_used: Instant) {
        self.last_used_at = last_used;
    }
}
