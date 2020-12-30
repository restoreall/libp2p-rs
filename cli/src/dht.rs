use async_std::task;
use libp2prs_core::{Multiaddr, PeerId};
use libp2prs_kad::Control;
use xcli::*;
use std::str::FromStr;

pub const DHT: &str = "dht";

// pub(crate) fn bootstrap(app: &App, _actions: &[&str]) -> XcliResult {
//     let mut value_any = app.get_handler(DHT).clone().expect("get kad controller failed");
//     let mut kad = match value_any.downcast_ref::<Control>() {
//         Some(ctrl) => ctrl.clone(),
//         None => {
//             println!("downcast failed");
//             return CmdExeCode::BadSyntax
//         }
//     };
//
//     task::block_on(async {
//         kad.bootstrap().await;
//         println!("add node completed");
//     });
//
//     CmdExeCode::Ok
// }
//
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
//
// pub(crate) fn rm_node(app: &App, actions: &[&str]) -> CmdExeCode {
//     let mut value_any = app.get_handler(DHT).clone().expect("get kad controller failed");
//     let mut kad = match value_any.downcast_ref::<Control>() {
//         Some(ctrl) => ctrl.clone(),
//         None => {
//             println!("downcast failed");
//             return CmdExeCode::BadSyntax
//         }
//     };
//
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
//     task::block_on(async {
//         kad.remove_node(peer).await;
//         println!("remove node completed");
//     });
//
//     CmdExeCode::Ok
// }
//
// pub(crate) fn list_all_node(app: &App, _actions: &[&str]) -> CmdExeCode {
//     let mut value_any = app.get_handler(DHT).clone().expect("get kad controller failed");
//     let mut kad = match value_any.downcast_ref::<Control>() {
//         Some(ctrl) => ctrl.clone(),
//         None => {
//             println!("downcast failed");
//             return CmdExeCode::BadSyntax
//         }
//     };
//
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
// pub(crate) fn get_value(app: &App, actions: &[&str]) -> CmdExeCode {
//     let mut value_any = app.get_handler(DHT).clone().expect("get kad controller failed");
//     let mut kad = match value_any.downcast_ref::<Control>() {
//         Some(ctrl) => ctrl.clone(),
//         None => {
//             println!("downcast failed");
//             return CmdExeCode::BadSyntax
//         }
//     };
//     let key = match actions.get(0).cloned() {
//         Some(k) => k,
//         None => return CmdExeCode::BadSyntax
//     };
//
//     task::block_on(async {
//         let value = kad.get_value(Vec::from(key)).await;
//         println!("get value: {:?}", value);
//     });
//
//     CmdExeCode::Ok
// }
//
// pub(crate) fn find_peer(app: &App, actions: &[&str]) -> CmdExeCode {
//     let mut value_any = app.get_handler(DHT).clone().expect("get kad controller failed");
//     let mut kad = match value_any.downcast_ref::<Control>() {
//         Some(ctrl) => ctrl.clone(),
//         None => {
//             println!("downcast failed");
//             return CmdExeCode::BadSyntax
//         }
//     };
//
//     let pid = match actions.get(0).cloned() {
//         Some(p) => p,
//         None => return CmdExeCode::BadSyntax
//     };
//     let peer = match PeerId::from_str(pid) {
//         Ok(p) => p,
//         Err(_) => return CmdExeCode::BadArgument(Some(String::from("invalid peer id")))
//     };
//
//     task::block_on(async {
//         let r = kad.find_peer(&peer).await;
//         println!("FindPeer: {:?}", r);
//     });
//
//     CmdExeCode::Ok
// }