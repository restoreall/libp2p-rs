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

use async_std::task;
use libp2prs_core::identity::Keypair;
use libp2prs_core::transport::upgrade::TransportUpgrade;
use libp2prs_core::upgrade::Selector;
use libp2prs_core::{Multiaddr, PeerId};
use libp2prs_mplex as mplex;
use libp2prs_secio as secio;
use libp2prs_swarm::Control as SwarmControl;
use libp2prs_swarm::identify::IdentifyConfig;
use libp2prs_swarm::Swarm;
use libp2prs_tcp::TcpConfig;
use libp2prs_yamux as yamux;
use libp2prs_kad::Control as KadControl;
use libp2prs_kad::kad::Kademlia;
use libp2prs_kad::store::MemoryStore;
use std::time::Duration;
use std::str::FromStr;


use xcli::*;
use libp2prs_multiaddr::protocol::Protocol;

struct MyCliData {
    kad: KadControl,
    swarm: SwarmControl,
}

fn main() {
    env_logger::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let port = std::env::args().nth(1).expect("usage: ./kad_nodes.exe <listen_port>");
    let listen_port = port.parse::<u16>().expect("string to u16");

    let keys = Keypair::generate_ed25519();
    let mut listen_addr: Multiaddr = "/ip4/127.0.0.1".parse().unwrap();
    listen_addr.push(Protocol::Tcp(listen_port));
    let (swarm_control, kad_control) = setup_kad(keys, listen_addr);

    // now preprare CLI and run it
    let mydata = MyCliData {
        swarm: swarm_control,
        kad: kad_control,
    };

    let mut app = App::new("xCLI", mydata)
        .version("v0.1")
        .author("kingwel.xie@139.com");

    // DHT
    let find_peer_cmd = Command::new("findpeer")
        .about("find peer through dht")
        .usage("findpeer <peerid>")
        .action(find_peer);
    let get_value_cmd = Command::new("getvalue")
        .about("get value through dht")
        .usage("getvalue <key>")
        .action(get_value);

    let dht_cmd = Command::new("dht")
        .about("find peer or record through dht")
        .usage("dht")
        .subcommand(find_peer_cmd)
        .subcommand(get_value_cmd);
    app.add_subcommand(dht_cmd);

    // Bootstrap
    let add_node_cmd = Command::new("add")
        .about("Add peers to the bootstrap list")
        .usage("bootstrap add [<peer>]")
        .action(add_node);

    let bootstrap_cmd = Command::new("bootstrap")
        .about("Show or edit the list of bootstrap peers")
        .usage("bootstrap")
        .subcommand(add_node_cmd);
    app.add_subcommand(bootstrap_cmd);



    app.add_subcommand(Command::new("swarm")
        .about("show Swarm information")
        .usage("swarm")
        .action(get_network_info));

    app.run();
}

fn add_node(app: &App<MyCliData>, actions: &Vec<&str>) -> CmdExeCode {
    let pid = match actions.get(0).cloned() {
        Some(p) => p,
        None => return CmdExeCode::BadSyntax
    };

    let addr = match actions.get(1).cloned() {
        Some(a) => a,
        None => return CmdExeCode::BadSyntax
    };

    let peer = match PeerId::from_str(pid) {
        Ok(p) => p,
        Err(e) => return CmdExeCode::BadArgument(Some(String::from("invalid peer id")))
    };

    let address = match Multiaddr::from_str(addr) {
        Ok(a) => a,
        Err(e) => return CmdExeCode::BadArgument(Some(String::from("invalid multi address")))
    };

    let userdata = app.get_userdata();
    let mut kad = userdata.kad.clone();
    task::block_on(async {
        kad.add_node(peer, vec![address]).await;
        println!("add node completed");
    });

    CmdExeCode::Ok
}

fn rm_node(app: &App<MyCliData>, actions: &Vec<&str>) -> CmdExeCode {
    let pid = match actions.get(0).cloned() {
        Some(p) => p,
        None => return CmdExeCode::BadSyntax
    };

    let peer = match PeerId::from_str(pid) {
        Ok(p) => p,
        Err(e) => return CmdExeCode::BadArgument(Some(String::from("invalid peer id")))
    };


    let userdata = app.get_userdata();
    let mut kad = userdata.kad.clone();
    task::block_on(async {
        kad.remove_node(peer).await;
        println!("remove node completed");
    });

    CmdExeCode::Ok
}

fn list_all_node(app: &App<MyCliData>, actions: &Vec<&str>) -> CmdExeCode {
    let userdata = app.get_userdata();
    let mut kad = userdata.kad.clone();
    task::block_on(async {
        kad.remove_node(peer).await;
        println!("remove node completed");
    });

    CmdExeCode::Ok
}

fn get_value(app: &App<MyCliData>, actions: &Vec<&str>) -> CmdExeCode {
    let key = match actions.get(0).cloned() {
        Some(k) => k,
        None => return CmdExeCode::BadSyntax
    };
    let userdata = app.get_userdata();
    let mut kad = userdata.kad.clone();
    task::block_on(async {
        let value = kad.get_value(Vec::from(key)).await;
        println!("get value: {:?}", value);
    });

    CmdExeCode::Ok
}

fn find_peer(app: &App<MyCliData>, actions: &Vec<&str>) -> CmdExeCode {
    let pid = match actions.get(0).cloned() {
        Some(p) => p,
        None => return CmdExeCode::BadSyntax
    };
    let peer = match PeerId::from_str(pid) {
        Ok(p) => p,
        Err(e) => return CmdExeCode::BadArgument(Some(String::from("invalid peer id")))
    };
    let userdata = app.get_userdata();
    let mut kad = userdata.kad.clone();
    task::block_on(async {
        let r = kad.find_peer(&peer).await;
        println!("FindPeer: {:?}", r);
    });

    CmdExeCode::Ok
}

fn get_network_info(app: &App<MyCliData>, actions: &Vec<&str>) -> CmdExeCode {
    let userdata = app.get_userdata();
    let mut swarm = userdata.swarm.clone();
    task::block_on(async {
        let r = swarm.retrieve_networkinfo().await;
        println!("NetworkInfo: {:?}", r);
        println!("Metric: {:?} {:?}", swarm.get_recv_count_and_size(), swarm.get_sent_count_and_size());
        //println!("AddrBook: {:?}", swarm.get_addrs_vec());
    });
    CmdExeCode::Ok
}

lazy_static! {
    static ref SERVER_KEY: Keypair = Keypair::generate_ed25519_fixed();
}

fn setup_kad(keys: Keypair, listen_addr: Multiaddr) -> (SwarmControl, KadControl) {
    let sec = secio::Config::new(keys.clone());
    let mux = Selector::new(yamux::Config::new(), mplex::Config::new());
    let tu = TransportUpgrade::new(TcpConfig::default(), mux.clone(), sec.clone());

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

    let pid = keys.public().into_peer_id();
    log::info!("I can be reached at: {}/{}", listen_addr, pid);

    (swarm_ctrl, kad_ctrl)
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
