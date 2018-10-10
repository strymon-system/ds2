// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::f64;
use std::str::FromStr;
use std::collections::{HashMap};

use dataflow::petgraph::graph::{NodeIndex};
use dataflow::petgraph::algo::toposort;

use dataflow::topology::Topology;
use dataflow::{OperatorId,OperatorInstanceId,OperatorInstances,Epoch,Rate};


/// Converts a dataflow configuration `conf` to a vector of pairs `(OperatorId,OperatorInstances)`
pub fn as_vec(conf: &String) -> Vec<(OperatorId,OperatorInstances)>
{
    let v = conf.split(",").collect::<Vec<&str>>();
    assert_eq!(v.len() % 2,0);
    let mut c = Vec::new();
    let mut it = v.chunks(2);
    while let Some(&[o,i]) = it.next()
    {
        c.push((o.to_string(),OperatorInstances::from_str(i).expect("Error parsing number of instances in configuration.")))
    }
    c
}

/// Collects all observation windows (epochs) that did not include any scale-up decision
///
/// # Arguments
///
/// `before`: The given dataflow configuration
///
/// `after`: The estimated optimal configuration
///
/// `sources`: The number of source operators in the dataflow
///
/// # Returns
///
/// A vector of epochs (identified by their start timestamps)
fn unchanged_or_scaled_down(before: HashMap<OperatorInstanceId,(NodeIndex,u32)>, after: HashMap<OperatorInstanceId,Vec<u32>>, sources: usize) -> Vec<Epoch>
{
    let mut min_epoch = usize::max_value();
    after.iter().for_each(|(_,instances)|
                    {
                        if min_epoch > instances.len() { min_epoch = instances.len(); }
                    });
    let mut epochs = Vec::new();
    for i in 0..min_epoch
    {
        let mut ops = 0;
        for (op1,instances_per_epoch) in after.iter()
        {
            let &(_,instances_before) = before.get(op1).expect("Operators before and after don't match");
            if instances_per_epoch[i] <= instances_before { ops += 1; }
        }
        if ops == before.len()-sources { epochs.push(i as Epoch); }
    }
    epochs
}

/// Evaluates the scaling model on a topology with collected metrics
///
/// # Arguments
///
/// `Topology`: The topology struct with the collected operator rates
///
/// `threshold`: The target rate ratio threshold
///
/// `factor`: The scaling factor
///
/// `verbose`: A boolean flag to print further information to standard output
///
/// # Returns
///
/// A string of the form:
///
/// `operator_id1,number_of_instances1,operator_id2,number_of_instances2,...`
///
/// containing the optimal dataflow configuration, i.e. the optimal level of parallelism for each dataflow operator
///
/// # Comments
///
/// * In case the collected rates in `Topology` correspond to multiple observation windows (epochs), the returned value contains the optimal dataflow configuration per time window
///
/// * The `threshold >= 1 (default: f64::MAX)` defines the maximum allowed ratio of a target source rate to the respective observed source rate after the application of a scaling decision.
/// When DS2 converges and the aforementioned ratio for a source operator is above the given threshold, a final configuration adjustment is triggered in an attempt to meet the throughput
///
/// * The `factor >= 0 (default: 0)` is used to deliberately over-provision each dataflow operator by `factor * 100%` more instances than the optimal
pub fn evaluate_scaling_policy(topo: &mut Topology, threshold: f64, factor: f64, verbose: bool) -> String
{
    // Used in configuration adjustments
    let sources = topo.get_sources_idx();
    let config_before = topo.dictionary.clone();
    let mut config_after = HashMap::new();
    let mut source_rates_ratios = Vec::new();
    // Useful in Timely experiments
    let mut workers_per_epoch = HashMap::new();
    let mut optimal_config = String::new();     // The result
    // Sort operators in topological order -- Works only with DAG topologies
    let mut operators_to_visit = toposort(&topo.logical_graph,None).expect("Cannot sort operators topologically.");
    let graph = &mut topo.logical_graph;        // The dataflow topology
    if verbose
    {
        print!("\nOperators in topological order: ");
        operators_to_visit.iter()
                        .for_each(|&op_id|
                        {
                            let op_info = graph.node_weight(op_id).unwrap();
                            print!("{}, ",op_info.name);
                        });
    }
    // A mapping from operators/nodes to their true output rates per observation window (epoch)
    let mut true_output_rate: HashMap<NodeIndex,Vec<(Epoch,Rate)>> = HashMap::new();
    for &idx in operators_to_visit.iter()
    { // Traverse operators/nodes in topological order, from the sources to the sinks
      // At each step, compare the true processing rate of the current operator with
      // the true output rate of the upstream operator(s) to estimate its optimal level of parallelism
        let mut true_processing_rate: Vec<(Epoch,Rate)> = Vec::new();
        { // Step 0: Collect the aggregated true processing rate for the current operator per epoch
            let op_info = &graph[idx];
            if verbose { println!("\n\nOperator '{}' with ID: {}",op_info.name,op_info.id); }
            for (&epoch,rates) in op_info.rates.iter()
            {
                true_processing_rate.push((epoch,rates.0));
            }
            true_processing_rate.sort_by(|a,b| a.0.cmp(&b.0));	// Sort by epoch
            if verbose { true_processing_rate.iter().for_each(|&(e,r)| println!("Aggregated true processing rate for epoch {} is {}",e,r)); }
        }
        let mut estimated_true_output_rate = Vec::new();
        let mut observed_output_rate = Vec::new();	// Used only in estimating source rate ratios
        { // Step 1: Collect the aggregated true output rate for the current operator per epoch
            let op_info = &graph[idx];
            for (&epoch,rates) in op_info.rates.iter()
            {
                estimated_true_output_rate.push((epoch,rates.1));
            }
            estimated_true_output_rate.sort_by(|a,b| a.0.cmp(&b.0));	// Sort by epoch
            // Compute 'true output rate / observed output rate' for each source operator per epoch
            if sources.contains(&idx)
            { // Keep source rate info per epoch
                for (&epoch,rates) in op_info.rates.iter() { observed_output_rate.push((epoch,rates.3)); }
                observed_output_rate.sort_by(|a,b| a.0.cmp(&b.0));	// Sort by epoch
                let ratios = estimated_true_output_rate.iter()
                                                    .zip(observed_output_rate.iter())
                                                    .map(|(&(_,r1),&(_,r2))| (r1/r2))
                                                    .collect::<Vec<f64>>();
                source_rates_ratios.push(ratios);
            }
        }
        // Step 2: Estimate the optimal level of parallelism for the current operator
        let op_instances = graph[idx].instances;
        if let Some(mut operator_input) = true_output_rate.remove(&idx)
        { // Initially, 'true_output_rates' is empty since the source operators do not have inputs
            for (epoch,input_rate) in operator_input.drain(..)
            {
                if epoch >= true_processing_rate.len() as u64 {break;}	// Ignore partially logged epochs
                if verbose { println!("Aggregated target rate for epoch {} is {}",epoch,input_rate); }
                // Estimate optimal level of parallelism assuming there is no computation skew across current operator instances
                let optimal_instances_per_epoch = input_rate/(true_processing_rate[epoch as usize].1 / op_instances as f64);
                // Update the total number of instances in the optimal dataflow configuration per epoch -- Useful in Timely experiments
                if !optimal_instances_per_epoch.is_nan() && !optimal_instances_per_epoch.is_infinite() // Ignore partially logged epochs
                {
                    let slot = workers_per_epoch.entry(epoch).or_insert(0f64);
                    *slot += optimal_instances_per_epoch;
                }
                // Store optimal configuration for the current operator in the topology
                graph[idx].optimal_parallelism_per_epoch.insert(epoch,(optimal_instances_per_epoch * (1.0 + factor)).ceil() as u32);
                // Estimate the aggregated true output rate of the current operator when configured with the optimal number of instances
                estimated_true_output_rate[epoch as usize].1 *= optimal_instances_per_epoch / (op_instances as f64);
            }
        }
        let adjacent_operators = graph.neighbors(idx).collect::<Vec<_>>();	// The downstream operators of the current operator
        for &operator in adjacent_operators.iter()
        { // Step 3: Update the estimated true output rate, which amounts to the estimated input rate of the downstream operators
            let entry = true_output_rate.entry(operator).or_insert_with(|| vec![(Epoch::default(),Rate::default());estimated_true_output_rate.len()]);
            for &(epoch,rate) in estimated_true_output_rate.iter()
            {
                if epoch >= entry.len() as u64 {break;}	// Ignore partially logged epochs
                entry[epoch as usize].0 =  epoch;
                entry[epoch as usize].1 += rate;
            }
        }
        {
            // Step 4: Add the optimal configuration of the current operator per epoch to the result
            let op_info = &graph[idx];
            for (&epoch, &level_of_parallelism) in op_info.optimal_parallelism_per_epoch.iter()
            {
                if !sources.contains(&idx)
                {
                    let e = config_after.entry(op_info.id.clone()).or_insert_with(|| Vec::new());
                    e.push(level_of_parallelism);
                }
                if verbose { println!("Optimal parallelism for epoch {} is {}",epoch,level_of_parallelism); }
                optimal_config.push_str(&format!("{},{},",op_info.id,level_of_parallelism))
            }
        }
    }
    if verbose
    { // Useful in Timely experiments
        let mut workers_per_epoch = workers_per_epoch.drain().collect::<Vec<_>>();
        workers_per_epoch.sort_by(|a,b| a.1.partial_cmp(&b.1).unwrap());    // Sort epochs by number of workers
        let mid = workers_per_epoch.len()/2;                                // The median number of workers across all epochs
        println!("Median number of workers (total): {}",workers_per_epoch[mid].1.ceil());
        workers_per_epoch.sort_by(|a,b| a.0.cmp(&b.0));	// Sort by epoch
        for (e,workers) in workers_per_epoch.drain(..)
        {
            println!("Epoch {}: {} -> {} workers",e,workers,workers.ceil());
        }
    }
    // Step 5: Check if there is a need for a final configuration adjustment
    if verbose { println!("Source rate ratios: {:?}",source_rates_ratios); }
    let epochs = source_rates_ratios[0].len();
    let mut max_ratios = vec!(0f64;epochs);
    // Compute the max 'true output rate / observed output rate' per epoch across all source operators
    source_rates_ratios.drain(..)
                    .for_each(|ratios|
                        {
                            let l1 = max_ratios.len();
                            let l2 = ratios.len();
                            if l1 < l2 { max_ratios.extend_from_slice(&ratios[l1..]); }
                            for i in 0..l1
                            {
                                if i >= l2 { break; }	// Ignore partially logged epochs
                                if max_ratios[i] < ratios[i] { max_ratios[i] = ratios[i]; }
                            }
                        });
    if verbose {  println!("Max source rate ratios per epoch ({}): {:?}",max_ratios.len(), max_ratios); }
    // Collect all epochs that did not include any scale-up decision with respect to the current configuration
    let epochs = unchanged_or_scaled_down(config_before,config_after,sources.len());
    if epochs.len() > 0
    { // There are epochs that might need a final configuration adjustment
        optimal_config.clear();
        for idx in operators_to_visit.drain(..)
        {
            let op_info = &graph[idx];
            for (&epoch, &level_of_parallelism) in op_info.optimal_parallelism_per_epoch.iter()
            {	// The adjusted number of instances for the current operator per epoch
                let epoch = epoch as usize;
                let parallelism_adjustment = match  epoch < max_ratios.len() &&         // Ignore partially logged epochs
                                                    max_ratios[epoch] > threshold &&    // Maximum target rate ratio is above the given threshold
                                                    epochs.contains(&(epoch as Epoch))  // Epoch did not include any scale-up decision
                                            {
                                                true => (max_ratios[epoch]*level_of_parallelism as f64).ceil() as u32,	// Apply a final configuration adjustment assuming that the
                                                                                                                        // operator's capacity increases linearly with its number of instances
                                                false => level_of_parallelism	// Leave the estimated value as is
                                            };
                // Update result with the adjusted optimal configuration
                optimal_config.push_str(&format!("{},{},",op_info.id,parallelism_adjustment));
                if verbose { println!("[Adjustment]: Optimal parallelism of operator {} for epoch {} is {}",op_info.id,epoch,parallelism_adjustment); }
            }
        }
    }
    let l = optimal_config.len();
    optimal_config.truncate(l-1);
    optimal_config
}

/// Evaluates the scaling model on a topology with collected metrics for a given epoch
///
/// # Arguments
///
/// `Topology`: The topology struct with the collected operator rates
///
/// `threshold`: The target rate ratio threshold
///
/// `factor`: The scaling factor
///
/// `epoch`: The epoch for which to evaluate the policy
///
/// `verbose`: A boolean flag to print further information to standard output
///
/// # Returns
///
/// A string of the form:
///
/// `operator_id1,number_of_instances1,operator_id2,number_of_instances2,...`
///
/// containing the optimal dataflow configuration, i.e. the optimal level of parallelism for each dataflow operator
///
/// # Comments
///
/// * The `threshold >= 1 (default: f64::MAX)` defines the maximum allowed ratio of a target source rate to the respective observed source rate after the application of a scaling decision.
/// When DS2 converges and the aforementioned ratio for a source operator is above the given threshold, a final configuration adjustment is triggered in an attempt to meet the throughput
///
/// * The `factor >= 0 (default: 0)` is used to deliberately over-provision each dataflow operator by `factor * 100%` more instances than the optimal
pub fn evaluate_scaling_policy_at_epoch(topo: &mut Topology, threshold: f64, factor: f64, epoch: Epoch, verbose: bool) -> String
{
    // Used in configuration adjustments
    let sources = topo.get_sources_idx();
    let mut source_rates_ratios = Vec::new();
    // Useful in Timely experiments
    let mut workers = 0f64;
    let mut optimal_config = String::new(); // The result
    // Sort operators in topological order -- Works only with DAG topologies
    let operators_to_visit = toposort(&topo.logical_graph,None).expect("Cannot sort operators topologically.");
    let graph = &mut topo.logical_graph;    // The dataflow topology
    if verbose
    {
        print!("\nOperators in topological order: ");
        operators_to_visit.iter()
                        .for_each(|&op_id|
                        {
                            let op_info = graph.node_weight(op_id).unwrap();
                            print!("{}, ",op_info.name);
                        });
    }
    // A mapping from operators/nodes to their true output rates per observation window (epoch)
    let mut true_output_rate: HashMap<NodeIndex,Rate> = HashMap::new();
    for &idx in operators_to_visit.iter()
    { // Traverse operators/nodes in topological order, from the sources to the sinks
      // At each step, compare the true processing rate of the current operator with the
      // true output rate of the upstream operator(s) to estimate its optimal level of parallelism
        let mut true_processing_rate = Rate::default();
        { // Step 0: Collect the aggregated true processing rate for the current operator per epoch
            let op_info = &graph[idx];
            if verbose { println!("\n\nOperator '{}' with ID: {}",op_info.name,op_info.id); }
            let rates = op_info.rates.get(&epoch).expect("No rates found for the given epoch.");
            true_processing_rate = rates.0;
            if verbose { println!("Aggregated true processing rate for epoch {} is {}",epoch,true_processing_rate); }
        }
        let mut estimated_true_output_rate = Rate::default();
        let mut observed_output_rate = Rate::default();	// Used only in estimating source rate ratios
        { // Step 1: Collect the aggregated true output rate for the current operator per epoch
            let op_info = &graph[idx];
            let rates = op_info.rates.get(&epoch).expect("No rates found for the given epoch.");
            estimated_true_output_rate = rates.1;
            // Compute 'true output rate / observed output rate' for each source operator per epoch
            if sources.contains(&idx)
            { // Keep source rate info
                let rates = op_info.rates.get(&epoch).expect("No rates found for the given epoch.");
                observed_output_rate = rates.3;
                let ratio = estimated_true_output_rate/observed_output_rate;
                source_rates_ratios.push(ratio);
            }
        }
        // Step 2: Estimate the optimal level of parallelism for the current operator
        let op_instances = graph[idx].instances;
        if let Some(mut input_rate) = true_output_rate.remove(&idx)
        { // Initially, 'true_output_rates' is empty since the source operators do not have inputs
            if verbose { println!("Aggregated target rate for epoch {} is {}",epoch,input_rate); }
            // Estimate optimal level of parallelism assuming there is no computation skew across current operator instances
            let optimal_instances = input_rate/(true_processing_rate / op_instances as f64);
            // Update the total number of instances in the optimal dataflow configuration per epoch -- Useful in Timely experiments
            if !optimal_instances.is_nan() && !optimal_instances.is_infinite() // Ignore partially logged epochs
            {
                workers += optimal_instances;
            }
            // Store optimal configuration for the current operator in the topology
            graph[idx].optimal_parallelism_per_epoch.insert(epoch, (optimal_instances * (1.0 + factor)).ceil() as u32);
            // Estimate the aggregated true output rate of the current operator when configured with the optimal number of instances
            estimated_true_output_rate *= optimal_instances / (op_instances as f64);
        }
        let adjacent_operators = graph.neighbors(idx).collect::<Vec<_>>();	// The downstream operators of the current operator
        for &operator in adjacent_operators.iter()
        { // Step 3: Update the estimated true output rate, which amounts to the estimated input rate of the downstream operators
            let entry = true_output_rate.entry(operator).or_insert(Rate::default());
            *entry += estimated_true_output_rate;
        }
        // Step 4: Add the optimal configuration of the current operator for the given epoch to the result
        let op_info = &graph[idx];
        match op_info.optimal_parallelism_per_epoch.get(&epoch)
        {
            Some(level_of_parallelism) =>
            {
                if verbose { println!("Optimal parallelism for epoch {} is {}",epoch,level_of_parallelism); }
                optimal_config.push_str(&format!("{},{},",op_info.id,level_of_parallelism))
            },
            None => { if !sources.contains(&idx) { panic!("No optimal parallelism found."); } }
        }
    }
    // Useful in Timely experiments
    if verbose { println!("Epoch {}: {} -> {} workers",epoch,workers,workers.ceil()); }
    // Add source parallelism to the result
    for &idx in sources.iter()
    {
        let src_id = &graph[idx].id;
        let level_of_parallelism = &graph[idx].instances;
        optimal_config.push_str(&format!("{},{},",src_id,level_of_parallelism))
    }
    let l = optimal_config.len();
    optimal_config.truncate(l-1);
    optimal_config
}
