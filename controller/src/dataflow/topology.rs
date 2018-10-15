// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate petgraph;

use std::collections::{HashMap,HashSet};
use self::petgraph::graph::{Graph,NodeIndex};

use dataflow::{Epoch,Rates,ChannelId,OperatorId,OperatorInstances};

#[derive(Debug)]
pub struct Operator
{
	pub id: OperatorId,
	pub name: String,
	pub instances: OperatorInstances,                              			// Number of operator instances (current operator parallelism)
	pub rates: HashMap<Epoch,Rates>,                               			// Processing and output rates of the operator per epoch
	pub optimal_parallelism_per_epoch: HashMap<Epoch,OperatorInstances>,	// Estimated optimal number of instances per epoch
}

impl Operator
{
    // Constructor
    pub fn new(op_id: &str , op_name: &str, op_instances: OperatorInstances) -> Self
    {
        Operator
        {
            id: op_id.to_string(),
            name: op_name.to_string(),
            instances: op_instances,
            rates: HashMap::new(),
            optimal_parallelism_per_epoch: HashMap::new()
        }
    }
}

pub struct Flow
{
    // TODO (john) 
}

pub struct Topology
{
    // The logical graph topology
    pub logical_graph: Graph<Operator,Flow>,
    // A dictionary from operator ids to internal indexes
    pub dictionary: HashMap<OperatorId,(NodeIndex,OperatorInstances)>,
    // A dictionary from Timely's channel ids to pairs of operator ids (source,target)
    pub channel_dictionary: HashMap<ChannelId,(OperatorId,OperatorId)>
}

impl Topology
{
    // Constructor
    pub fn new() -> Self
    {
        Topology
        {
            logical_graph: Graph::new(),
            dictionary: HashMap::new(),
            channel_dictionary: HashMap::new()
        }
    }

    /// Adds an operator/node to the topology and returns its internal index. If the node exists, it simply returns its index
    ///
    /// # Arguments
    ///
    /// `id`: The operator id
    ///
    /// `name`: The name of the operator
    ///
    /// `instances`: The total number of operator instances
    pub fn add_unique_node(&mut self, id: &str, name: &str, instances: u32) -> NodeIndex
    {
        let d = &mut self.dictionary;
        let lg = &mut self.logical_graph;
        let idx = d.entry(id.to_string())
                    .or_insert_with(||
                        {
                            let op1 = Operator::new(id,name,instances);
                            (lg.add_node(op1),instances)
                        }
                    );
        // Registered instances must match the provided number
        assert_eq!(instances,idx.1);
        match lg.node_weight(idx.0)
        { // Registered name must match the provided one
            Some(op_info) => assert_eq!(op_info.name,name.to_string()),
            None => panic!("Operator not found in topology.")
        }
        idx.0
    }

    /// Adds an edge/channel to the topology if it does not exist
    ///
    /// # Arguments
    ///
    /// `source_node`: The internal index of the source operator/node
    ///
    /// `target_node`: The internal index of the target operator/node
    pub fn add_unique_edge(&mut self, source_node: NodeIndex, target_node: NodeIndex)
    {
        let _ = self.logical_graph.update_edge(source_node,target_node,Flow{});
    }

    /// Returns the internal index of an operator/node
    ///
    /// # Arguments
    ///
    /// `op_id`: The id of the operator/node
    pub(super) fn get_node_idx(&self, op_id: &OperatorId) -> NodeIndex
    {
        match self.dictionary.get(op_id)
        {
            Some((idx,_)) => *idx,
            None => panic!("No node with id '{}' found in topology.",op_id)
        }
    }

    /// Returns the internal indexes of all source operators/nodes in the topology
    pub fn get_sources_idx(&self) -> HashSet<NodeIndex>
    {
        let mut sources = HashSet::new();
        let mut targets = HashSet::new();

        let edges = &self.logical_graph.raw_edges();
        for edge in edges.iter()
        {
            sources.insert(edge.source());
            targets.insert(edge.target());
        }
        let ids: HashSet<NodeIndex> = sources.difference(&targets).map(|x| x.clone()).collect();
        if ids.len() == 0 { panic!("No sources found in topology."); }
        ids
    }

    /// Clears all rate information
    pub fn clear_rates(&mut self)
    {
        let lg = &mut self.logical_graph;
        for op_info in lg.node_weights_mut()
        {
            op_info.rates.clear();
        }
    }

    /// Clears everything
    pub fn clear(&mut self)
    {
        self.logical_graph.clear();
        self.dictionary.clear();
        self.channel_dictionary.clear();
    }

    /// Retrieves the current dataflow configuration
    pub fn get_configuration(&self, exclude_sources: bool) -> Vec<(OperatorId,OperatorInstances)>
    {
		if exclude_sources
		{
			let sources = self.get_sources_idx();
			self.dictionary.iter().filter(|(_,(idx,_))| !sources.contains(idx))
									.map(|(operator,(_,instances))| (operator.clone(),*instances))
									.collect()
		}
		else { self.dictionary.iter().map(|(operator,(_,instances))| (operator.clone(),*instances)).collect() }
    }

    /// Sets the dataflow configuration
    pub fn set_configuration(&mut self, conf: &Vec<(OperatorId,OperatorInstances)>)
    {
      for (ref op, inst) in conf.iter()
      {
        let slot = self.dictionary.get_mut(op).expect("Operator not found in topology.");
        slot.1 = *inst;
		self.logical_graph[slot.0].instances = *inst;
      }
    }

    /// Prints the topology to standard output
    pub fn print(&self)
    {
        println!("\nLogical Dataflow");
        println!("================");
        println!("\nOperators:");
        for n in self.logical_graph.raw_nodes()
        {
            println!("{:?}",n);
        }
        println!("\nChannels:");
        for e in self.logical_graph.raw_edges()
        {
            println!("{:?},{:?}",self.logical_graph.node_weight(e.source()),self.logical_graph.node_weight(e.target()));
        }
    }
}
