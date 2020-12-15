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

//! The internal API for a single `KBucket` in a `KBucketsTable`.
//!
//! > **Note**: Uniqueness of entries w.r.t. a `Key` in a `KBucket` is not
//! > checked in this module. This is an invariant that must hold across all
//! > buckets in a `KBucketsTable` and hence is enforced by the public API
//! > of the `KBucketsTable` and in particular the public `Entry` API.

pub use crate::K_VALUE;
use super::*;

//
// /// The status of a node in a bucket.
// ///
// /// The status of a node in a bucket together with the time of the
// /// last status change determines the position of the node in a
// /// bucket.
// #[derive(PartialEq, Eq, Debug, Copy, Clone)]
// pub enum NodeStatus {
//     /// The node is considered connected.
//     Connected,
//     /// The node is considered disconnected.
//     Disconnected
// }

/// A `Node` in a bucket, representing a peer participating
/// in the Kademlia DHT together with an associated value (e.g. contact
/// information).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node<TKey, TVal> {
    /// The key of the node, identifying the peer.
    pub key: TKey,
    /// The associated value.
    pub value: TVal,
}

/// The position of a node in a `KBucket`, i.e. a non-negative integer
/// in the range `[0, K_VALUE)`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Position(usize);

/// A `KBucket` is a list of up to `K_VALUE` keys and associated values,
/// ordered from least-recently connected to most-recently connected.
#[derive(Debug, Clone)]
pub struct KBucket<TKey, TVal> {
    /// The nodes contained in the bucket.
    nodes: ArrayVec<[Node<TKey, TVal>; K_VALUE.get()]>,

    // /// The position (index) in `nodes` that marks the first connected node.
    // ///
    // /// Since the entries in `nodes` are ordered from least-recently connected to
    // /// most-recently connected, all entries above this index are also considered
    // /// connected, i.e. the range `[0, first_connected_pos)` marks the sub-list of entries
    // /// that are considered disconnected and the range
    // /// `[first_connected_pos, K_VALUE)` marks sub-list of entries that are
    // /// considered connected.
    // ///
    // /// `None` indicates that there are no connected entries in the bucket, i.e.
    // /// the bucket is either empty, or contains only entries for peers that are
    // /// considered disconnected.
    // first_connected_pos: Option<usize>,
}

/// The result of inserting an entry into a bucket.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertResult {
     /// The entry has been successfully inserted.
     Inserted,
     /// The entry is pending insertion because the relevant bucket is currently full.
     /// The entry is inserted after a timeout elapsed, if the status of the
     /// least-recently connected (and currently disconnected) node in the bucket
     /// is not updated before the timeout expires.
     /// The entry was not inserted because the relevant bucket is full.
     Full
}

impl<TKey, TVal> KBucket<TKey, TVal>
where
    TKey: Clone + AsRef<KeyBytes>,
    TVal: Clone
{
    /// Creates a new `KBucket` with the given timeout for pending entries.
    pub fn new() -> Self {
        KBucket {
            nodes: ArrayVec::new(),
        }
    }

    /// Returns a reference to a node in the bucket.
    pub fn get(&self, key: &TKey) -> Option<&Node<TKey, TVal>> {
        self.position(key).map(|p| &self.nodes[p.0])
    }

    /// Returns an iterator over the nodes in the bucket, together with their status.
    pub fn iter(&self) -> impl Iterator<Item = &Node<TKey, TVal>> {
        self.nodes.iter()
    }

    /// Inserts a new node into the bucket with the given status.
    pub fn insert(&mut self, node: Node<TKey, TVal>) -> bool {
        if self.nodes.is_full() {
            false
        } else {
            self.nodes.push(node);
            true
        }
    }

    /// Removes the node with the given key from the bucket, if it exists.
    pub fn remove(&mut self, key: &TKey) -> Option<(Node<TKey, TVal>, Position)> {
        if let Some(pos) = self.position(key) {
            // Remove the node from its current position.
            let node = self.nodes.remove(pos.0);
            Some((node, pos))
        } else {
            None
        }
    }

    /// Gets the number of entries currently in the bucket.
    pub fn num_entries(&self) -> usize {
        self.nodes.len()
    }

    /// Gets the position of an node in the bucket.
    pub fn position(&self, key: &TKey) -> Option<Position> {
        self.nodes.iter().position(|p| p.key.as_ref() == key.as_ref()).map(Position)
    }

    /// Gets a mutable reference to the node identified by the given key.
    ///
    /// Returns `None` if the given key does not refer to a node in the
    /// bucket.
    pub fn get_mut(&mut self, key: &TKey) -> Option<&mut Node<TKey, TVal>> {
        self.nodes.iter_mut().find(move |p| p.key.as_ref() == key.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use libp2prs_core::PeerId;
    use rand::Rng;
    use std::collections::VecDeque;
    use super::*;
    use quickcheck::*;

    impl Arbitrary for KBucket<Key<PeerId>, ()> {
        fn arbitrary<G: Gen>(g: &mut G) -> KBucket<Key<PeerId>, ()> {
            let timeout = Duration::from_secs(g.gen_range(1, g.size() as u64));
            let mut bucket = KBucket::<Key<PeerId>, ()>::new(timeout);
            let num_nodes = g.gen_range(1, K_VALUE.get() + 1);
            for _ in 0 .. num_nodes {
                let key = Key::new(PeerId::random());
                let node = Node { key: key.clone(), value: () };
                let status = NodeStatus::arbitrary(g);
                match bucket.insert(node, status) {
                    InsertResult::Inserted => {}
                    _ => panic!()
                }
            }
            bucket
        }
    }

    impl Arbitrary for NodeStatus {
        fn arbitrary<G: Gen>(g: &mut G) -> NodeStatus {
            if g.gen() {
                NodeStatus::Connected
            } else {
                NodeStatus::Disconnected
            }
        }
    }

    impl Arbitrary for Position {
        fn arbitrary<G: Gen>(g: &mut G) -> Position {
            Position(g.gen_range(0, K_VALUE.get()))
        }
    }

    // Fill a bucket with random nodes with the given status.
    fn fill_bucket(bucket: &mut KBucket<Key<PeerId>, ()>, status: NodeStatus) {
        let num_entries_start = bucket.num_entries();
        for i in 0 .. K_VALUE.get() - num_entries_start {
            let key = Key::new(PeerId::random());
            let node = Node { key, value: () };
            assert_eq!(InsertResult::Inserted, bucket.insert(node, status));
            assert_eq!(bucket.num_entries(), num_entries_start + i + 1);
        }
    }

    #[test]
    fn ordering() {
        fn prop(status: Vec<NodeStatus>) -> bool {
            let mut bucket = KBucket::<Key<PeerId>, ()>::new(Duration::from_secs(1));

            // The expected lists of connected and disconnected nodes.
            let mut connected = VecDeque::new();
            let mut disconnected = VecDeque::new();

            // Fill the bucket, thereby populating the expected lists in insertion order.
            for status in status {
                let key = Key::new(PeerId::random());
                let node = Node { key: key.clone(), value: () };
                let full = bucket.num_entries() == K_VALUE.get();
                match bucket.insert(node, status) {
                    InsertResult::Inserted => {
                        let vec = match status {
                            NodeStatus::Connected => &mut connected,
                            NodeStatus::Disconnected => &mut disconnected
                        };
                        if full {
                            vec.pop_front();
                        }
                        vec.push_back((status, key.clone()));
                    }
                    _ => {}
                }
            }

            // Get all nodes from the bucket, together with their status.
            let mut nodes = bucket.iter()
                .map(|(n, s)| (s, n.key.clone()))
                .collect::<Vec<_>>();

            // Split the list of nodes at the first connected node.
            let first_connected_pos = nodes.iter().position(|(s,_)| *s == NodeStatus::Connected);
            assert_eq!(bucket.first_connected_pos, first_connected_pos);
            let tail = first_connected_pos.map_or(Vec::new(), |p| nodes.split_off(p));

            // All nodes before the first connected node must be disconnected and
            // in insertion order. Similarly, all remaining nodes must be connected
            // and in insertion order.
            nodes == Vec::from(disconnected)
                &&
            tail == Vec::from(connected)
        }

        quickcheck(prop as fn(_) -> _);
    }

    #[test]
    fn full_bucket() {
        let mut bucket = KBucket::<Key<PeerId>, ()>::new(Duration::from_secs(1));

        // Fill the bucket with disconnected nodes.
        fill_bucket(&mut bucket, NodeStatus::Disconnected);

        // Trying to insert another disconnected node fails.
        let key = Key::new(PeerId::random());
        let node = Node { key, value: () };
        match bucket.insert(node, NodeStatus::Disconnected) {
            InsertResult::Full => {},
            x => panic!("{:?}", x)
        }

        // One-by-one fill the bucket with connected nodes, replacing the disconnected ones.
        for i in 0 .. K_VALUE.get() {
            let (first, first_status) = bucket.iter().next().unwrap();
            let first_disconnected = first.clone();
            assert_eq!(first_status, NodeStatus::Disconnected);

            // Add a connected node, which is expected to be pending, scheduled to
            // replace the first (i.e. least-recently connected) node.
            let key = Key::new(PeerId::random());
            let node = Node { key: key.clone(), value: () };
            match bucket.insert(node.clone(), NodeStatus::Connected) {
                InsertResult::Pending { disconnected } =>
                    assert_eq!(disconnected, first_disconnected.key),
                x => panic!("{:?}", x)
            }

            // Trying to insert another connected node fails.
            match bucket.insert(node.clone(), NodeStatus::Connected) {
                InsertResult::Full => {},
                x => panic!("{:?}", x)
            }

            assert!(bucket.pending().is_some());

            // Apply the pending node.
            let pending = bucket.pending_mut().expect("No pending node.");
            pending.set_ready_at(Instant::now() - Duration::from_secs(1));
            let result = bucket.apply_pending();
            assert_eq!(result, Some(AppliedPending {
                inserted: node.clone(),
                evicted: Some(first_disconnected)
            }));
            assert_eq!(Some((&node, NodeStatus::Connected)), bucket.iter().last());
            assert!(bucket.pending().is_none());
            assert_eq!(Some(K_VALUE.get() - (i + 1)), bucket.first_connected_pos);
        }

        assert!(bucket.pending().is_none());
        assert_eq!(K_VALUE.get(), bucket.num_entries());

        // Trying to insert another connected node fails.
        let key = Key::new(PeerId::random());
        let node = Node { key, value: () };
        match bucket.insert(node, NodeStatus::Connected) {
            InsertResult::Full => {},
            x => panic!("{:?}", x)
        }
    }

    #[test]
    fn full_bucket_discard_pending() {
        let mut bucket = KBucket::<Key<PeerId>, ()>::new(Duration::from_secs(1));
        fill_bucket(&mut bucket, NodeStatus::Disconnected);
        let (first, _) = bucket.iter().next().unwrap();
        let first_disconnected = first.clone();

        // Add a connected pending node.
        let key = Key::new(PeerId::random());
        let node = Node { key: key.clone(), value: () };
        if let InsertResult::Pending { disconnected } = bucket.insert(node, NodeStatus::Connected) {
            assert_eq!(&disconnected, &first_disconnected.key);
        } else {
            panic!()
        }
        assert!(bucket.pending().is_some());

        // Update the status of the first disconnected node to be connected.
        bucket.update(&first_disconnected.key, NodeStatus::Connected);

        // The pending node has been discarded.
        assert!(bucket.pending().is_none());
        assert!(bucket.iter().all(|(n,_)| &n.key != &key));

        // The initially disconnected node is now the most-recently connected.
        assert_eq!(Some((&first_disconnected, NodeStatus::Connected)), bucket.iter().last());
        assert_eq!(bucket.position(&first_disconnected.key).map(|p| p.0), bucket.first_connected_pos);
        assert_eq!(1, bucket.num_connected());
        assert_eq!(K_VALUE.get() - 1, bucket.num_disconnected());
    }


    #[test]
    fn bucket_update() {
        fn prop(mut bucket: KBucket<Key<PeerId>, ()>, pos: Position, status: NodeStatus) -> bool {
            let num_nodes = bucket.num_entries();

            // Capture position and key of the random node to update.
            let pos = pos.0 % num_nodes;
            let key = bucket.nodes[pos].key.clone();

            // Record the (ordered) list of status of all nodes in the bucket.
            let mut expected = bucket.iter().map(|(n,s)| (n.key.clone(), s)).collect::<Vec<_>>();

            // Update the node in the bucket.
            bucket.update(&key, status);

            // Check that the bucket now contains the node with the new status,
            // preserving the status and relative order of all other nodes.
            let expected_pos = match status {
                NodeStatus::Connected => num_nodes - 1,
                NodeStatus::Disconnected => bucket.first_connected_pos.unwrap_or(num_nodes) - 1
            };
            expected.remove(pos);
            expected.insert(expected_pos, (key.clone(), status));
            let actual = bucket.iter().map(|(n,s)| (n.key.clone(), s)).collect::<Vec<_>>();
            expected == actual
        }

        quickcheck(prop as fn(_,_,_) -> _);
    }
}
