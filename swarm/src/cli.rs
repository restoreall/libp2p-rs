use crate::Control;
use async_std::task;
use xcli::*;
use libp2prs_core::PeerId;
use std::convert::TryFrom;

pub const SWRM: &str = "swarm";

pub fn add_swarm_commands(app: &mut App, swarm: Control) {
    // register handler
    app.register(SWRM, Box::new(swarm));

    // add sub commands
    let swarm_cmd = Command::new(SWRM)
        .about("Swarm commands")
        .usage("swarm")
        .subcommand(
            Command::new("show")
                .about("displays swarm basic information")
                .usage("connection [PeerId]")
                .action(cli_show_basic),
        )
        .subcommand(
            Command::new("connection")
                .about("displays connection information")
                .usage("connection [PeerId]")
                .action(cli_show_connections),
        )
        .subcommand(
            Command::new("peer")
                .about("displays peer information")
                .usage("peer [PeerId]")
                .action(cli_show_peers),
        );

    app.add_subcommand(swarm_cmd);
}

fn handler(app: &App) -> Control {
    let value_any = app.get_handler(SWRM).expect(SWRM);
    let swarm = value_any.downcast_ref::<Control>().expect("control").clone();
    swarm
}

fn cli_show_basic(app: &App, _args: &[&str]) -> XcliResult {
    let mut swarm = handler(app);
    task::block_on(async {
        let r = swarm.retrieve_networkinfo().await;
        println!("NetworkInfo: {:?}", r);

        let r = swarm.retrieve_identify_info().await;
        println!("IdentifyInfo: {:?}", r);

        let addresses = swarm.self_addrs().await;
        println!("Addresses: {:?}", addresses);

        println!(
            "Metric: {:?} {:?}",
            swarm.get_recv_count_and_size(),
            swarm.get_sent_count_and_size()
        );
    });

    Ok(CmdExeCode::Ok)
}

fn cli_show_connections(app: &App, args: &[&str]) -> XcliResult {
    let mut swarm = handler(app);

    let peer = match args.len() {
        0 => None,
        1 => Some(PeerId::try_from(args[0]).map_err(|e|XcliError::BadArgument(e.to_string()))?),
        _ => return Err(XcliError::MismatchArgument(1, args.len())),
    };

    task::block_on(async {
        let connections = swarm.dump_connections(peer).await.unwrap();
        println!("CID   DIR Remote-Peer-Id                                       I/O  Remote-Multiaddr");
        connections.iter().for_each(|v| {
            println!(
                "{} {} {:52} {}/{}  {}",
                v.id, v.dir, v.info.remote_peer_id, v.info.num_inbound_streams, v.info.num_outbound_streams, v.info.ra
            );
            if true {
                v.substreams.iter().for_each(|s| {
                    println!("      ({})", s);
                });
            }
        });
    });

    Ok(CmdExeCode::Ok)
}


fn cli_show_peers(app: &App, args: &[&str]) -> XcliResult {
    let swarm = handler(app);

    let pid = match args.len() {
        0 => None,
        1 => Some(PeerId::try_from(args[0]).map_err(|e|XcliError::BadArgument(e.to_string()))?),
        _ => return Err(XcliError::MismatchArgument(1, args.len())),
    };

    if let Some(peer) = pid {
        let addrs = swarm.get_addrs_vec(&peer);
        let protos = swarm.get_protocol(&peer);
        println!("Addrs: {:?}\nProtocols: {:?}", addrs, protos);
    } else {
        let peers = swarm.get_all_peers();
        println!("Remote-Peer-Id                                       Multiaddrs");
        peers.iter().for_each(|v| {
            println!("{:52} {:?}", v, swarm.get_addrs_vec(v));
        });
    }
    Ok(CmdExeCode::Ok)
}
