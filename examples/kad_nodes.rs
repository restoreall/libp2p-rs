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

//use async_std::task;
#[macro_use]
extern crate lazy_static;

use libp2prs_core::identity::Keypair;
use libp2prs_core::transport::upgrade::TransportUpgrade;
use libp2prs_core::upgrade::Selector;
use libp2prs_core::{Multiaddr, PeerId};
use libp2prs_mplex as mplex;
use libp2prs_secio as secio;
use libp2prs_swarm::identify::IdentifyConfig;
use libp2prs_swarm::Swarm;
use libp2prs_tcp::TcpConfig;
use libp2prs_yamux as yamux;
use libp2prs_kad::kad::Kademlia;
use libp2prs_kad::store::MemoryStore;
use std::time::Duration;
use std::str::FromStr;

fn main() {
    env_logger::from_env(env_logger::Env::default().default_filter_or("info")).init();
    if std::env::args().nth(1) == Some("server".to_string()) {
        log::info!("Starting server ......");
        run_server();
    } else {
        log::info!("Starting client ......");
        run_client(std::env::args().nth(1));
    }
}

lazy_static! {
    static ref SERVER_KEY: Keypair = Keypair::generate_ed25519_fixed();
}

#[allow(clippy::empty_loop)]
fn run_server() {
    let keys = SERVER_KEY.clone();

    let sec = secio::Config::new(keys.clone());
    let mux = Selector::new(yamux::Config::new(), mplex::Config::new());
    let tu = TransportUpgrade::new(TcpConfig::default(), mux.clone(), sec.clone());

    let mut swarm = Swarm::new(keys.public())
        .with_transport(Box::new(tu))
        .with_identify(IdentifyConfig::new(false));

    log::info!("Swarm created, local-peer-id={:?}", swarm.local_peer_id());

    let store = MemoryStore::new(swarm.local_peer_id().clone());
    let kad = Kademlia::new(swarm.local_peer_id().clone(), store);
    let kad_handler = kad.handler();
    kad.start(swarm.control());

    let listen_addr: Multiaddr = "/ip4/0.0.0.0/tcp/8086".parse().unwrap();
    swarm.listen_on(vec![listen_addr]).unwrap();
    swarm = swarm.with_protocol(Box::new(kad_handler));
    swarm.start();

    loop {
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}

fn run_client(peer: Option<String>) {
    let keys = Keypair::generate_secp256k1();

    let sec = secio::Config::new(keys.clone());
    let mux = Selector::new(yamux::Config::new(), mplex::Config::new());
    let tu = TransportUpgrade::new(TcpConfig::default(), mux.clone(), sec.clone());

    let mut swarm = Swarm::new(keys.public())
        .with_transport(Box::new(tu))
        .with_identify(IdentifyConfig::new(false));
    let mut swarm_ctrl = swarm.control();

    log::info!("Swarm created, local-peer-id={:?}", swarm.local_peer_id());

    let remote_peer_id = PeerId::from_public_key(SERVER_KEY.public());
    log::info!("connect to peer {:?}", remote_peer_id);

    let store = MemoryStore::new(swarm.local_peer_id().clone());
    let kad = Kademlia::new(swarm.local_peer_id().clone(), store);
    let kad_handler = kad.handler();
    let mut kad_ctrl = kad.control();
    kad.start(swarm.control());

    if peer.is_none() {
        let listen_addr: Multiaddr = "/ip4/0.0.0.0/tcp/8087".parse().unwrap();
        swarm.listen_on(vec![listen_addr]).unwrap();
    }
    swarm = swarm.with_protocol(Box::new(kad_handler));

    swarm.start();

    async_std::task::block_on(async {
        swarm_ctrl.add_addr(&remote_peer_id, "/ip4/127.0.0.1/tcp/8086".parse().unwrap(), Duration::default(), true);
        swarm_ctrl.new_connection(remote_peer_id.clone()).await.expect("new connection");

        if let Some(peer) = peer {
            // wait for identify result
            async_std::task::sleep(std::time::Duration::from_secs(1)).await;

            let peer = PeerId::from_str(&peer).expect("invalid peer");
            log::info!("find peer: {:?}", peer);
            let addrs = kad_ctrl.find_peer(&peer).await.expect("DHT find peer");
            for addr in &addrs.multiaddrs {
                log::info!("addr: {}", addr);
            }
        }

        loop {
            async_std::task::sleep(std::time::Duration::from_secs(5)).await;
        }
    });
}
