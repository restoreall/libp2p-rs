use libp2prs_swarm::Control;
use async_std::task;
use xcli::*;

pub const SWRM: &str = "swarm";

pub(crate) fn get_network_info(app: &App, _actions: &Vec<&str>) -> XcliResult {
    let mut value_any = app.get_handler(SWRM).clone().expect("get swarm controller failed");
    let mut swarm = match value_any.downcast_ref::<Control>() {
        Some(ctrl) => ctrl.clone(),
        None => {
            println!("downcast failed");
            return Err(XcliError::BadSyntax.into());
        }
    };

    task::block_on(async {
        let r = swarm.retrieve_networkinfo().await?;
        println!("NetworkInfo: {:?}", r);
        println!("Metric: {:?} {:?}", swarm.get_recv_count_and_size(), swarm.get_sent_count_and_size());
        let addresses = swarm.retrieve_all_addrs().await?;
        println!("Addresses: {:?}", addresses);
        Ok(CmdExeCode::Ok)
    })
}