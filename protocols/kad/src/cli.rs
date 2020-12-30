use async_std::task;
use libp2prs_core::{Multiaddr, PeerId};
use crate::Control;
use xcli::*;
use std::str::FromStr;

pub const DHT: &str = "dht";

pub fn add_dht_commands(app: &mut App) {
    let bootstrap_cmd = Command::new("bootstrap")
        .about("Show or edit the list of bootstrap peers")
        .usage("bootstrap")
        .action(bootstrap);
    let add_node_cmd = Command::new("add")
        .about("Add peer to KBucket")
        .usage("add [<peer>] [<multi_address>]")
        .action(add_node);
    let rm_node_cmd = Command::new("rm")
        .about("Remove peer from KBucket")
        .usage("rm [<peer>] [<multi_address>]")
        .action(rm_node);
    let list_node_cmd = Command::new("list")
        .about("List all node from KBucket")
        .usage("list")
        .action(list_all_node);
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
        .subcommand(bootstrap_cmd)
        .subcommand(add_node_cmd)
        .subcommand(rm_node_cmd)
        .subcommand(list_node_cmd)
        .subcommand(find_peer_cmd)
        .subcommand(get_value_cmd);
    app.add_subcommand(dht_cmd);
}

pub(crate) fn bootstrap(app: &App, _actions: &[&str]) -> XcliResult {
    let value_any = app.get_handler(DHT)?;
    let mut kad = match value_any.downcast_ref::<Control>() {
        Some(ctrl) => ctrl.clone(),
        None => {
            println!("downcast failed");
            return Err(XcliError::BadSyntax)
        }
    };

    task::block_on(async {
        kad.bootstrap().await;
        println!("add node completed");
    });

    Ok(CmdExeCode::Ok)
}

pub(crate) fn add_node(app: &App, actions: &[&str]) -> XcliResult {
    let value_any = app.get_handler(DHT)?;
    let mut kad = match value_any.downcast_ref::<Control>() {
        Some(ctrl) => ctrl.clone(),
        None => {
            println!("downcast failed");
            return Err(XcliError::BadSyntax)
        }
    };

    let pid = actions.get(0).cloned().ok_or(XcliError::BadSyntax)?;
    let addr = actions.get(1).cloned().ok_or(XcliError::BadSyntax)?;

    let peer = PeerId::from_str(pid).map_err(|e| XcliError::BadArgument(Some(e.into())))?;
    let address = Multiaddr::from_str(addr).map_err(|e| XcliError::BadArgument(Some(e.into())))?;

    task::block_on(async {
        kad.add_node(peer, vec![address]).await;
        println!("add node completed");
    });

    Ok(CmdExeCode::Ok)
}

pub(crate) fn rm_node(app: &App, actions: &[&str]) -> XcliResult {
    let value_any = app.get_handler(DHT)?;
    let mut kad = match value_any.downcast_ref::<Control>() {
        Some(ctrl) => ctrl.clone(),
        None => {
            println!("downcast failed");
            return Err(XcliError::BadSyntax)
        }
    };

    let pid = actions.get(0).cloned().ok_or(XcliError::BadSyntax)?;
    let peer = PeerId::from_str(pid).map_err(|e| XcliError::BadArgument(Some(e.into())))?;

    task::block_on(async {
        kad.remove_node(peer).await;
        println!("remove node completed");
    });

    Ok(CmdExeCode::Ok)
}

pub(crate) fn list_all_node(app: &App, _actions: &[&str]) -> XcliResult {
    let value_any = app.get_handler(DHT)?;
    let mut kad = match value_any.downcast_ref::<Control>() {
        Some(ctrl) => ctrl.clone(),
        None => {
            println!("downcast failed");
            return Err(XcliError::BadSyntax)
        }
    };

    task::block_on(async {
        let peers = kad.list_all_node().await;
        println!("nodes:");
        for p in peers {
            println!("{:?}", p);
        }
        // can't work, why???
        // peers.iter().cloned().map(|p| println!("nodes: {:?}", p));
    });

    Ok(CmdExeCode::Ok)
}

pub(crate) fn get_value(app: &App, actions: &[&str]) -> XcliResult {
    let value_any = app.get_handler(DHT)?;
    let mut kad = match value_any.downcast_ref::<Control>() {
        Some(ctrl) => ctrl.clone(),
        None => {
            println!("downcast failed");
            return Err(XcliError::BadSyntax)
        }
    };
    let key = actions.get(0).cloned().ok_or(XcliError::BadSyntax)?;

    task::block_on(async {
        let value = kad.get_value(Vec::from(key)).await;
        println!("get value: {:?}", value);
    });

    Ok(CmdExeCode::Ok)
}

pub(crate) fn find_peer(app: &App, actions: &[&str]) -> XcliResult {
    let value_any = app.get_handler(DHT)?;
    let mut kad = match value_any.downcast_ref::<Control>() {
        Some(ctrl) => ctrl.clone(),
        None => {
            println!("downcast failed");
            return Err(XcliError::BadSyntax)
        }
    };

    let pid = actions.get(0).cloned().ok_or(XcliError::BadSyntax)?;
    let peer = PeerId::from_str(pid).map_err(|e| XcliError::BadArgument(Some(e.into())))?;

    task::block_on(async {
        let r = kad.find_peer(&peer).await;
        println!("FindPeer: {:?}", r);
    });

    Ok(CmdExeCode::Ok)
}