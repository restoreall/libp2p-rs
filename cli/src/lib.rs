mod dht;
mod swrm;

use libp2prs_swarm::Control as SwarmControl;
use libp2prs_kad::Control as KadControl;
use xcli::*;
use crate::swrm::SWRM;
use crate::dht::DHT;

pub struct Cli {
    app: App<'static>,
}

impl Cli {
    pub fn new(swarm: SwarmControl, kad: KadControl) -> Self {
        let mut app = App::new("xCLI")
            .version("v0.1")
            .author("kingwel.xie@139.com");

        app.register(SWRM, Box::new(swarm));
        app.register(DHT, Box::new(kad));

        Cli {app}
    }

    pub fn add_commands(&mut self) {
        self.add_swarm_cmd();
        self.add_dht_cmd();
    }

    fn add_swarm_cmd(&mut self) {
        self.app.add_subcommand(Command::new("swarm")
            .about("show Swarm information")
            .usage("swarm")
            .action(swrm::get_network_info));
    }

    fn add_dht_cmd(&mut self) {
        // let bootstrap_cmd = Command::new("bootstrap")
        //     .about("Show or edit the list of bootstrap peers")
        //     .usage("bootstrap")
        //     .action(dht::bootstrap);
        let add_node_cmd = Command::new("add")
            .about("Add peer to KBucket")
            .usage("add [<peer>] [<multi_address>]")
            .action(dht::add_node);
        // let rm_node_cmd = Command::new("rm")
        //     .about("Remove peer from KBucket")
        //     .usage("rm [<peer>] [<multi_address>]")
        //     .action(dht::rm_node);
        // let list_node_cmd = Command::new("list")
        //     .about("List all node from KBucket")
        //     .usage("list")
        //     .action(dht::list_all_node);
        // let find_peer_cmd = Command::new("findpeer")
        //     .about("find peer through dht")
        //     .usage("findpeer <peerid>")
        //     .action(dht::find_peer);
        // let get_value_cmd = Command::new("getvalue")
        //     .about("get value through dht")
        //     .usage("getvalue <key>")
        //     .action(dht::get_value);

        let dht_cmd = Command::new("dht")
            .about("find peer or record through dht")
            .usage("dht")
            // .subcommand(bootstrap_cmd)
            .subcommand(add_node_cmd);
            // .subcommand(rm_node_cmd)
            // .subcommand(list_node_cmd)
            // .subcommand(find_peer_cmd)
            // .subcommand(get_value_cmd);
        self.app.add_subcommand(dht_cmd);
    }

    pub fn run(self) {
        self.app.run()
    }
}

