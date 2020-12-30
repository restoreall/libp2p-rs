use async_std::task;
use xcli::*;
use crate::Control;

pub const SWRM: &str = "swarm";

pub fn add_swarm_commands(app: &mut App) {
    app.add_subcommand(Command::new("swarm")
        .about("show Swarm information")
        .usage("swarm")
        .action(get_network_info));
}

pub(crate) fn get_network_info(app: &App, _actions: &[&str]) -> XcliResult {
    let value_any = app.get_handler(SWRM)?;
    let mut swarm = match value_any.downcast_ref::<Control>() {
        Some(ctrl) => ctrl.clone(),
        None => {
            println!("downcast failed");
            return Err(XcliError::BadSyntax);
        }
    };

    task::block_on(async {
        let r = swarm.retrieve_networkinfo().await;
        println!("NetworkInfo: {:?}", r);

        println!("Metric: {:?} {:?}", swarm.get_recv_count_and_size(), swarm.get_sent_count_and_size());

        let addresses = swarm.self_addrs().await;
        println!("Addresses: {:?}", addresses);

        let connections = swarm.dump_connections(None).await.unwrap();
        println!("CID   DIR Remote-Peer-Id                                       I/O  Remote-Multiaddr");
        connections
            .iter()
            .for_each(|v| {
                println!("{} {} {:52} {}/{}  {}", v.id, v.dir, v.info.remote_peer_id,
                         v.info.num_inbound_streams, v.info.num_outbound_streams, v.info.ra);
                if true {
                    v.substreams.iter().for_each(|s| {
                        println!("      ({})", s);
                    });
                }
            });
    });

    Ok(CmdExeCode::Ok)
}