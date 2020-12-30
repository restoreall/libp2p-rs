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

use libp2prs_core::identity::Keypair;
use libp2prs_core::transport::upgrade::TransportUpgrade;
use libp2prs_core::upgrade::Selector;
use libp2prs_core::Multiaddr;
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


use libp2prs_cli::*;
use libp2prs_multiaddr::protocol::Protocol;

fn main() {
    env_logger::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let port = std::env::args().nth(1).expect("usage: ./kad_nodes.exe <listen_port>");
    let listen_port = port.parse::<u16>().expect("string to u16");

    let keys = Keypair::generate_ed25519();
    let mut listen_addr: Multiaddr = "/ip4/127.0.0.1".parse().unwrap();
    listen_addr.push(Protocol::Tcp(listen_port));
    let (swarm_control, kad_control) = setup_kad(keys, listen_addr);

    let mut app = Cli::new(swarm_control, kad_control);
    app.add_commands();
    app.run();
}

// const NO_DHT: &str = "dht is not supported";
//
// fn dht(app: &App<MyCliData>, _actions: &Vec<&str>) -> CmdExeCode {
//     let userdata = app.get_userdata();
//     userdata.kad.clone().map_or(CmdExeCode::BadArgument(Some(NO_DHT.to_string())), |_c| CmdExeCode::Ok)
// }
//
// fn bootstrap(app: &App<MyCliData>, _actions: &Vec<&str>) -> CmdExeCode {
//     let userdata = app.get_userdata();
//     let mut kad = userdata.kad.clone().expect("kad control");
//     task::block_on(async {
//         kad.bootstrap().await;
//         println!("add node completed");
//     });
//
//     CmdExeCode::Ok
// }
//
// fn add_node(app: &App<MyCliData>, actions: &Vec<&str>) -> CmdExeCode {
//     let pid = match actions.get(0).cloned() {
//         Some(p) => p,
//         None => return CmdExeCode::BadSyntax
//     };
//
//     let addr = match actions.get(1).cloned() {
//         Some(a) => a,
//         None => return CmdExeCode::BadSyntax
//     };
//
//     let peer = match PeerId::from_str(pid) {
//         Ok(p) => p,
//         Err(_) => return CmdExeCode::BadArgument(Some(String::from("invalid peer id")))
//     };
//
//     let address = match Multiaddr::from_str(addr) {
//         Ok(a) => a,
//         Err(_) => return CmdExeCode::BadArgument(Some(String::from("invalid multi address")))
//     };
//
//     let userdata = app.get_userdata();
//     let mut kad = userdata.kad.clone().expect("kad control");;
//     task::block_on(async {
//         kad.add_node(peer, vec![address]).await;
//         println!("add node completed");
//     });
//
//     CmdExeCode::Ok
// }
//
// fn rm_node(app: &App<MyCliData>, actions: &Vec<&str>) -> CmdExeCode {
//     let pid = match actions.get(0).cloned() {
//         Some(p) => p,
//         None => return CmdExeCode::BadSyntax
//     };
//
//     let peer = match PeerId::from_str(pid) {
//         Ok(p) => p,
//         Err(_) => return CmdExeCode::BadArgument(Some(String::from("invalid peer id")))
//     };
//
//     let userdata = app.get_userdata();
//     let mut kad = userdata.kad.clone().expect("kad control");;
//     task::block_on(async {
//         kad.remove_node(peer).await;
//         println!("remove node completed");
//     });
//
//     CmdExeCode::Ok
// }
//
// fn list_all_node(app: &App<MyCliData>, _actions: &Vec<&str>) -> CmdExeCode {
//     let userdata = app.get_userdata();
//     let mut kad = userdata.kad.clone().expect("kad control");;
//     task::block_on(async {
//         let peers = kad.list_all_node().await;
//         println!("nodes:");
//         for p in peers {
//             println!("{:?}", p);
//         }
//         // can't work, why???
//         // peers.iter().cloned().map(|p| println!("nodes: {:?}", p));
//     });
//
//     CmdExeCode::Ok
// }
//
// fn get_value(app: &App<MyCliData>, actions: &Vec<&str>) -> CmdExeCode {
//     let key = match actions.get(0).cloned() {
//         Some(k) => k,
//         None => return CmdExeCode::BadSyntax
//     };
//     let userdata = app.get_userdata();
//     let mut kad = userdata.kad.clone().expect("kad control");;
//     task::block_on(async {
//         let value = kad.get_value(Vec::from(key)).await;
//         println!("get value: {:?}", value);
//     });
//
//     CmdExeCode::Ok
// }
//
// fn find_peer(app: &App<MyCliData>, actions: &Vec<&str>) -> CmdExeCode {
//     let pid = match actions.get(0).cloned() {
//         Some(p) => p,
//         None => return CmdExeCode::BadSyntax
//     };
//     let peer = match PeerId::from_str(pid) {
//         Ok(p) => p,
//         Err(_) => return CmdExeCode::BadArgument(Some(String::from("invalid peer id")))
//     };
//     let userdata = app.get_userdata();
//     let mut kad = userdata.kad.clone().expect("kad control");;
//     task::block_on(async {
//         let r = kad.find_peer(&peer).await;
//         println!("FindPeer: {:?}", r);
//     });
//
//     CmdExeCode::Ok
// }
//
// fn get_network_info(app: &App<MyCliData>, _actions: &Vec<&str>) -> CmdExeCode {
//     let userdata = app.get_userdata();
//     let mut swarm = userdata.swarm.clone();
//     task::block_on(async {
//         let r = swarm.retrieve_networkinfo().await;
//         println!("NetworkInfo: {:?}", r);
//         println!("Metric: {:?} {:?}", swarm.get_recv_count_and_size(), swarm.get_sent_count_and_size());
//         let addresses = swarm.retrieve_all_addrs().await;
//         println!("Addresses: {:?}", addresses);
//     });
//     CmdExeCode::Ok
// }

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
    log::info!("I can be reached at: {}/p2p/{}", listen_addr, pid);

    (swarm_ctrl, kad_ctrl)
}
