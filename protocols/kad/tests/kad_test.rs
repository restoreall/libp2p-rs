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

use async_std::task;
use libp2prs_core::identity::Keypair;
use libp2prs_core::multiaddr::protocol::Protocol;
use libp2prs_core::transport::memory::MemoryTransport;
use libp2prs_core::transport::upgrade::TransportUpgrade;
use libp2prs_core::{Multiaddr, PeerId};
use libp2prs_kad::store::MemoryStore;
use libp2prs_kad::{kad::Kademlia, Control as kad_control};
use libp2prs_plaintext as plaintext;
use libp2prs_swarm::identify::IdentifyConfig;
use libp2prs_swarm::{Control as swarm_control, Swarm};
use libp2prs_yamux as yamux;
use quickcheck::{QuickCheck, TestResult};
use rand::random;
use std::time::Duration;

fn setup_kad(keys: Keypair, listen_addr: Multiaddr) -> (swarm_control, kad_control) {
    let sec = plaintext::PlainTextConfig::new(keys.clone());
    let mux = yamux::Config::new();
    let tu = TransportUpgrade::new(MemoryTransport::default(), mux, sec);

    let mut swarm = Swarm::new(keys.public())
        .with_transport(Box::new(tu))
        .with_identify(IdentifyConfig::new(false));

    log::info!("Swarm created, local-peer-id={:?}", swarm.local_peer_id());

    // start kad protocol
    let store = MemoryStore::new(swarm.local_peer_id().clone());
    let kad = Kademlia::new(swarm.local_peer_id().clone(), store);
    let kad_handler = kad.handler();
    let kad_ctrl = kad.control();
    kad.start(swarm.control());

    // register handler to swarm
    swarm = swarm.with_protocol(Box::new(kad_handler));

    swarm.listen_on(vec![listen_addr.clone()]).expect("listen on");
    let swarm_ctrl = swarm.control();
    swarm.start();

    (swarm_ctrl, kad_ctrl)
}

#[derive(Clone)]
struct PeerInfo {
    pid: PeerId,
    addr: Multiaddr,
    swarm_ctrl: swarm_control,
    kad_ctrl: kad_control,
}

fn setup_kads(n: usize) -> Vec<PeerInfo> {
    // Now swarm.close() do not close connection.
    // So use random port to protect from reuse port error
    let base_port = 1 + random::<u64>();
    let mut peers_info = vec![];
    for i in 0..n {
        let key = Keypair::generate_ed25519();
        let pid = key.public().into_peer_id();
        let addr: Multiaddr = Protocol::Memory(base_port + i as u64).into();

        let (swarm_ctrl, kad_ctrl) = setup_kad(key, addr.clone());
        peers_info.push(PeerInfo {
            pid,
            addr,
            swarm_ctrl,
            kad_ctrl,
        });
    }
    peers_info
}

async fn connect(a: &mut PeerInfo, b: &PeerInfo) {
    a.swarm_ctrl.add_addr(&b.pid, b.addr.clone(), Duration::default(), true);
    a.swarm_ctrl.new_connection(b.pid.clone()).await.expect("new connection");
}

#[test]
fn test_value_get_set() {
    fn prop() -> TestResult {
        task::block_on(async {
            let infos = setup_kads(5);
            let mut node0 = infos.get(0).expect("get peer info").clone();
            let mut node1 = infos.get(1).expect("get peer info").clone();
            // let mut node2 = infos.get(2).expect("get peer info").clone();

            connect(&mut node0, &node1).await;

            let key = b"/v/hello".to_vec();
            let value = b"world".to_vec();
            node0.kad_ctrl.put_value(key.clone(), value.clone()).await;

            task::sleep(Duration::from_millis(500)).await;

            let value1 = node1.kad_ctrl.get_value(key.clone()).await.expect("get value");
            assert_eq!(value, value1);

            TestResult::passed()

            // TODO: propagate value and provider record when new peer connected
            // connect to node0 and node1, verify if get_value success
            // connect(&mut node2, &node0);
            // connect(&mut node2, &node1);
            //
            // task::sleep(Duration::from_millis(500)).await;
            //
            // let value2 = node2.kad_ctrl.get_value(key.clone()).await.expect("get value");
            // assert_eq!(value, value2);
        })
    }
    env_logger::from_env(env_logger::Env::default().default_filter_or("info")).init();
    QuickCheck::new().tests(10).quickcheck(prop as fn() -> _);
}

#[test]
fn test_provides() {
    fn prop() -> TestResult {
        task::block_on(async {
            let infos = setup_kads(2);
            let mut node0 = infos.get(0).expect("get peer info").clone();
            let mut node1 = infos.get(1).expect("get peer info").clone();
            // let node2 = infos.get(2).expect("get peer info").clone();
            // let mut node3 = infos.get(3).expect("get peer info").clone();

            for info in infos.iter() {
                println!("{:?}", info.pid);
            }

            connect(&mut node0, &node1).await;
            // connect(&mut node1, &node2).await;
            // connect(&mut node1, &node3).await;

            // wait for identify result
            task::sleep(Duration::from_secs(1)).await;

            let key = b"hello".to_vec();
            node0.kad_ctrl.provide(key.clone()).await;

            task::sleep(Duration::from_secs(2)).await;

            let providers = node1.kad_ctrl.find_providers(key.clone(), 1).await.expect("can't find provider");
            for provider in providers {
                println!("provider {:?}", provider);
                assert_eq!(node0.pid, provider.node_id);
                assert_eq!(node0.addr, provider.multiaddrs.get(0).expect("get addr").clone());
            }

            TestResult::passed()

            // let providers = node1.kad_ctrl.find_providers(key.clone(), 1).await.expect("can't find provider");
            // for provider in providers {
            //     assert_eq!(node3.pid, provider.node_id);
            //     assert_eq!(node3.addr, provider.multiaddrs.get(0).expect("get addr").clone());
            // }
            //
            // let providers = node2.kad_ctrl.find_providers(key.clone(), 1).await.expect("can't find provider");
            // for provider in providers {
            //     assert_eq!(node3.pid, provider.node_id);
            //     assert_eq!(node3.addr, provider.multiaddrs.get(0).expect("get addr").clone());
            // }
        })
    }
    env_logger::from_env(env_logger::Env::default().default_filter_or("info")).init();
    QuickCheck::new().tests(10).quickcheck(prop as fn() -> _);
}

#[test]
fn test_find_peer() {
    fn prop() -> TestResult {
        task::block_on(async {
            let infos = setup_kads(5);
            let mut node0 = infos.get(0).expect("get peer info").clone();
            let mut node1 = infos.get(1).expect("get peer info").clone();
            let node2 = infos.get(2).expect("get peer info").clone();

            connect(&mut node0, &node1).await;
            connect(&mut node1, &node2).await;

            // node0 find node2
            let p = node0.kad_ctrl.find_peer(&node2.pid).await.expect("find peer");
            assert_eq!(p.node_id, node2.pid.clone());

            TestResult::passed()
        })
    }
    QuickCheck::new().tests(10).quickcheck(prop as fn() -> _);
}
