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
use libp2prs_swarm::{Swarm, Control as SwarmControl};
use libp2prs_tcp::TcpConfig;
//use libp2prs_traits::{ReadEx, WriteEx};
use libp2prs_kad::kad::Kademlia;
use libp2prs_kad::store::MemoryStore;
use libp2prs_yamux as yamux;
use libp2prs_kad::Control as KadControl;

use xcli::*;

struct MyCliData {
    kad: KadControl,
    swarm: SwarmControl,
}


fn main() {
    env_logger::from_env(env_logger::Env::default().default_filter_or("info")).init();
    if std::env::args().nth(1) == Some("server".to_string()) {
        log::info!("Starting server ......");
        run_server();
    } else {
        log::info!("Starting client ......");
        //run_client();
    }
}

lazy_static! {
    static ref SERVER_KEY: Keypair = Keypair::generate_ed25519_fixed();
}

#[allow(clippy::empty_loop)]
fn run_server() {
    let keys = SERVER_KEY.clone();

    let listen_addr1: Multiaddr = "/ip4/0.0.0.0/tcp/8086".parse().unwrap();

    let sec = secio::Config::new(keys.clone());
    let mux = Selector::new(yamux::Config::new(), mplex::Config::new());
    let tu = TransportUpgrade::new(TcpConfig::default(), mux.clone(), sec.clone());

    let mut swarm = Swarm::new(keys.public())
        .with_transport(Box::new(tu))
        .with_identify(IdentifyConfig::new(false));

    log::info!("Swarm created, local-peer-id={:?}", swarm.local_peer_id());

    let swarm_control = swarm.control();

    swarm.listen_on(vec![listen_addr1]).unwrap();

    let store = MemoryStore::new(swarm.local_peer_id().clone());
    let kad = Kademlia::new(swarm.local_peer_id().clone(), store);

    let kad_control = kad.control();

    swarm = swarm.with_protocol(Box::new(kad.handler()));
    kad.start(swarm_control.clone());

    swarm.start();

    // now preprare CLI and run it
    let mydata = MyCliData {
        kad: kad_control,
        swarm: swarm_control,
    };

    let mut app = App::new("xCLI", mydata)
        .version("v0.1")
        .author("kingwel.xie@139.com");

    app.add_subcommand(Command::new("qwert")
        .about("controls testing features")
        .usage("qwert")
        .action(|app, _| -> CmdExeCode {
            let userdata = app.get_userdata();
            let mut kad = userdata.kad.clone();
            async_std::task::block_on(async {
                let peer = PeerId::random();
                let r = kad.find_peer(&peer).await;

                println!("FindPeer: {:?}", r);
            });

            println!("qwert tested");
            CmdExeCode::Ok
        }));
    app.add_subcommand(Command::new("swarm")
        .about("show Swarm information")
        .usage("swarm")
        .action(|app, _| -> CmdExeCode {
            let userdata = app.get_userdata();
            let mut swarm = userdata.swarm.clone();
            async_std::task::block_on(async {
                let r = swarm.retrieve_networkinfo().await;
                println!("NetworkInfo: {:?}", r);

                println!("Metric: {:?} {:?}", swarm.get_recv_count_and_size(), swarm.get_sent_count_and_size());

                //println!("AddrBook: {:?}", swarm.get_addrs_vec());
            });
            CmdExeCode::Ok
        }));

    app.run();
}
