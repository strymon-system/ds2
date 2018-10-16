// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate regex;

use std::path::Path;
use std::f64;
use std::str::FromStr;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader};
use std::collections::{HashMap,HashSet};

use self::regex::Regex;

use dataflow::topology::Topology;
use dataflow::{Log,Rate,Rates,OperatorId,WorkerId,Epoch,OperatorInstanceId};


/******* Flink methods *******/

/// Parses a Flink dataflow from a CSV file and creates a topology
///
/// # Arguments
///
/// `file`: The path to the CSV file
///
/// # Comments
///
/// * Each dataflow operator is represented as a triple of the form:
///
///   `id,name,number_of_instances`
///
/// * Each line in `file` represents a directed edge from an upstream to a downstream operator and has the form:
///
///   `id1,name1,number_of_instances1,id2,name2,number_of_instances2`
pub fn create_flink_topology(file: &Path) -> Topology
{
    let mut topology = Topology::new();
    let file = File::open(&file).expect("Cannot open topology file.");
    let reader = BufReader::new(file);
    for line in reader.lines()
    {
        let line = line.unwrap();
        if line.len() == 0 || line.chars().next() == Some('#') { continue; }
        let fields: Vec<&str> = line.split(',').map(|x| x.trim()).collect();
        let inst1 = u32::from_str(&fields[2]).expect("Cannot parse the number of source operator instances.");
        let n1 = topology.add_unique_node(fields[0],fields[1],inst1);
        let inst2 = u32::from_str(&fields[5]).expect("Cannot parse the number of target operator instances.");
        let n2 = topology.add_unique_node(fields[3],fields[4],inst2);
        topology.add_unique_edge(n1,n2);
    }
    topology
}

/// Parses rates of Flink operators from a CSV file and stores them in a topology
///
/// # Arguments
///
/// `rates_file`: The path to the CSV file
///
/// `topology`: The topology struct to store the rate information
///
/// `source_rates_file`: The path to an optional CSV file containing true output rates of source operators
///
/// `verbose`: A boolean flag to print info to stdout
///
/// # Comments
///
/// * Each line in `file` contains the true and observed rates of an operator instance aggregated per time window. It has the form:
///
///   `operator_id,instance_id,total_number_of_operator_instances,window_start_timestamp,true_processing_rate,true_output_rate,observed_processing_rate,observed_output_rate`
///
/// * Each line in `source_rates_file` contains true output rates of source operators. It has the form:
///
///   `operator_id,true_output_rate_per_instance`
///
/// * The true output rates in `source_rates_file` overwrite the respective rates from `file` (if any)
pub fn read_flink_rates(rates_file: &Path, topology: &mut Topology, source_rates_file: Option<&Path>, verbose: bool)
{
    let logs = collect_logs(rates_file,topology);
    set_rates(topology,logs);
    // Set true output source rates explicitly
    match source_rates_file
    {
        Some(fp) =>
        {
            let source_rates = parse_operator_rates(fp,verbose);
            set_true_output_rates(topology,source_rates,verbose);
        },
        None => {}
    }
}

/// Parses operator rates from a CSV file and stores them in a dictionary
///
/// # Arguments
///
/// `rates_file`: The path to the CSV file
///
/// `topology`: The topology the rates correspond to
///
/// # Returns
///
/// A mapping from operators to rate logs per instance and epoch
///
/// # Comments
///
/// * The `topology` is only used to validate the given operator IDs and their number of instances
fn collect_logs(rates_file: &Path, topology: &Topology) -> HashMap<OperatorId,HashMap<OperatorInstanceId,Vec<Log>>>
{
    let file = File::open(&rates_file).expect("Cannot open rates file.");
    let reader = BufReader::new(file);
    let mut logs: HashMap<OperatorId,HashMap<OperatorInstanceId,Vec<Log>>> = HashMap::new();
    for line in reader.lines()
    {
        let line = line.unwrap();
        if line.trim().len() == 0 || line.chars().next()==Some('#') {continue;}
        let log: Vec<&str> = line.split(',').map(|x| x.trim()).collect();
        let num_of_instances = u32::from_str(log[2]).expect("Cannot extract number of operator instances.");
        let &(_,inst) = topology.dictionary.get(log[0]).expect("Operator not found in dictionary.");
        // Registered instances must match the provided number
        assert_eq!(num_of_instances,inst);
        // Collect rate information
        let operator_slot = logs.entry(log[0].to_string()).or_insert_with(|| HashMap::new());
        let instance_slot = operator_slot
                            .entry(log[1].to_string())
                            .or_insert_with(|| Vec::new());
        instance_slot.push((u64::from_str(log[3]).expect("Cannot extract epoch timestamp."),
                            f64::from_str(log[4]).expect("Cannot extract true processing rate."),
                            f64::from_str(log[5]).expect("Cannot extract true output rate."),
                            f64::from_str(log[6]).expect("Cannot extract observed processing rate."),
                            f64::from_str(log[7]).expect("Cannot extract observed output rate."))
                    );
    }
    logs
}

/// Sets operator rates in a topology
///
/// # Arguments
///
/// `topology`: The topology struct to store the rates
///
/// `logs`: A mapping from operators to rate logs per instance and epoch
///
/// # Comments
///
/// * All previously recorded rates for each operator in `logs` are cleared
fn set_rates(topology: &mut Topology, mut logs: HashMap<OperatorId,HashMap<OperatorInstanceId,Vec<Log>>>)
{
    // Aggregate rates per logical operator
    for (op_id,instance_logs) in logs.iter_mut()
    {
        let mut op_logs: HashMap<Epoch,Rates> = HashMap::new();
        for (_,mut logs) in instance_logs.iter_mut()
        { // For each logical operator with logs for N epochs, the latter are
          // numbered as 0,1,2,...N in the order of their actual timestamps
            let mut ep = 0;
            logs.sort_by(|a:&Log,b:&Log| a.0.cmp(&b.0)); // Sort epochs according to their timestamp
            for &(_,true_proc_rate,true_out_rate,obs_proc_rate,obs_out_rate) in logs.iter()
            {
                let op_log = op_logs.entry(ep).or_insert_with(|| Rates::default());
                op_log.0 += true_proc_rate;
                op_log.1 += true_out_rate;
                op_log.2 += obs_proc_rate;
                op_log.3 += obs_out_rate;
                ep += 1;
            }
        }
        // Update topology with aggregated logs
        let &(idx,_) = topology.dictionary.get(op_id).expect("Operator not found in dictionary.");
        match topology.logical_graph.node_weight_mut(idx)
        {
            Some(op_info) => { op_info.rates = op_logs; }
            None => panic!("Operator not found in topology.")
        }
    }
}

/// Updates operator rates for a given epoch
///
/// # Arguments
///
/// `topology`: The topology struct to update
///
/// `logs`: A mapping from operators to rate logs per instance
///
/// `epoch`: The epoch number the `logs` correspond to
///
/// # Comments
///
/// * No previously recorded rates are cleared; rates in `logs` are simply added to the existing ones (if any)
fn update_rates(topology: &mut Topology, mut logs: HashMap<OperatorId,HashMap<OperatorInstanceId,Vec<Log>>>, epoch: Epoch)
{
    for (op_id,instance_logs) in logs.iter_mut()
    { // Aggregate rates per logical operator
        let &(idx,_) = topology.dictionary.get(op_id).expect("Operator not found in dictionary.");
        match topology.logical_graph.node_weight_mut(idx)
        {
            Some(ref mut op_info) =>
            {
                let op_logs = &mut op_info.rates;
                for (_,mut logs) in instance_logs.iter_mut()
                { // Make sure the given logs correspond to a single epoch, i.e. the given one
                    assert_eq!(logs.len(),1);
                    let mut ep = epoch;
                    for &(_,true_proc_rate,true_out_rate,obs_proc_rate,obs_out_rate) in logs.iter()
                    {// Add given rates to the existing ones (if any)
                        let op_log = op_logs.entry(ep).or_insert_with(|| Rates::default());
                        op_log.0 += true_proc_rate;
                        op_log.1 += true_out_rate;
                        op_log.2 += obs_proc_rate;
                        op_log.3 += obs_out_rate;
                        ep += 1;
                    }
                }
            }
            None => panic!("Operator not found in topology.")
        };
    }
}

/// Parses rates of Flink operators from a CSV file and updates a topology
///
/// # Arguments
///
/// `rates_file`: The path to the CSV file
///
/// `topology`: The topology struct to update with the rate information
///
/// `epoch`: The epoch number the rates correspond to
///
/// # Comments
///
/// * Each line in `rates_file` contains the true and observed rates of an operator instance aggregated per time window. It has the form:
///
///   `operator_id,instance_id,total_number_of_operator_instances,window_start_timestamp,true_processing_rate,true_output_rate,observed_processing_rate,observed_output_rate`
pub fn update_flink_rates(rates_file: &Path, topology: &mut Topology, epoch: Epoch)
{
    let logs = collect_logs(rates_file,topology);
    update_rates(topology,logs,epoch);
}


/******* Heron methods *******/

/// Parses a Heron dataflow from a CSV file and creates a topology
///
/// # Arguments
///
/// * `file`: The path to the CSV file
///
/// # Comments
///
/// * Each dataflow operator is represented as a triple of the form:
///
///   `id,name,number_of_instances`
///
/// * Each line in `file` represents a directed edge from an upstream to a downstream operator and has the form:
///
///   `id1,name1,number_of_instances1,id2,name2,number_of_instances2`
pub fn create_heron_topology(file: &Path) -> Topology
{ // Heron's and Flink's physical topologies have the same format
    create_flink_topology(file)
}

/// Parses rates of Heron operators from a CSV file and stores them in a topology
///
/// # Arguments
///
/// `file`: The path to the CSV file
///
/// `topology`: The topology struct to store the rate information
///
/// `source_rates_file`: The path to an optional CSV file containing true output rates of source operators
///
/// `operator_rates_file`: The path to an optional CSV file containing true processing rates of operators
///
/// `verbose`: A boolean flag to print further information to standard output
///
/// # Comments
///
/// * Each line in `file` contains the true and observed rates of an operator instance aggregated per time window. It has the form:
///
///   `operator_id,instance_id,number_of_operator_instances,window_start_timestamp,true_processing_rate,true_output_rate,observed_processing_rate,observed_output_rate`
///
/// * Each line in `source_rates_file` contains true output rates of source operators. It has the form:
///
///   `operator_id,true_output_rate_per_instance`
///
/// * Each line in `operator_rates_file` contains true processing rates of operators. It has the form:
///
///   `operator_id,true_processing_rate_per_instance`
///
/// * The true output rates in `source_rates_file` and `operator_rates_file` overwrite the respective rates from `file` (if any)
pub fn read_heron_rates(file: &Path, topology: &mut Topology, source_rates_file: Option<&Path>, operator_rates_file: Option<&Path>,  verbose: bool)
{
    read_flink_rates(file,topology,source_rates_file,verbose);
    // Set true output source rates explicitly
    match source_rates_file
    {
        Some(fp) =>
        {
            let source_rates = parse_operator_rates(fp,verbose);
            set_true_output_rates(topology,source_rates,verbose);
        },
        None => {}
    }
    // Set true processing rates explicitly
    match operator_rates_file
    {
        Some(fp) =>
        {
            let operator_rates = parse_operator_rates(fp,verbose);
            set_true_processing_rates(topology,operator_rates,verbose);
        },
        None => {}
    }
}

/// Sets the true processing rates of operators in a topology
///
/// # Arguments
///
/// `topology`: The topology struct to update
///
/// `operator_rates`: A vector of (`OperatorId`,`Rate`) pairs, where
/// `OperatorId` is the id of an operator and
/// `Rate` is its true processing rate per time window
///
/// `verbose`: A boolean flag to print further information to standard output
///
/// # Comments
///
/// * This function is used to replicate Dhallion's wordcount experiment by setting the true processing rate of operators to a max value
///
/// * If there are true processing rates for `OperatorId` that correspond to multiple epochs, all of them are set to the given `Rate` value
///
/// * The function can be used with `Rate = -1f64` to set the true processing rate of an operator with id `OperatorId` equal to its observed processing rate, as stored in `Topology`
fn set_true_processing_rates(topology: &mut Topology, operator_rates: Vec<(OperatorId,Rate)>, verbose: bool)
{
    for (ref op_id, ref op_proc_rate) in operator_rates
    { // There are max operator processing rates. Use these if the logged values are larger
        let &(op_idx,_) = topology.dictionary.get(op_id).expect("Operator not found in dictionary");
        match topology.logical_graph.node_weight_mut(op_idx)
        {
            Some(op_info) =>
            {
                if *op_proc_rate == -1f64
                { // Set the true processing rate of src_id equal to its observed processing rate. It indicates there is no waiting in this operator.
                    if verbose { println!("\nSetting the aggregated true processing rate of the operator (per epoch) equal to the aggregated observed processing rate."); }
                    for (_,mut rates) in op_info.rates.iter_mut()
                    {
                        rates.0 = rates.2;
                    }
                }
                else { // Set the true processing rates to the provided values
                    // TODO: What if there are no recorded rates?
                    let inst = op_info.instances;
                    let agg_op_rate = op_proc_rate * inst as f64;
                    let mut epoch = 0;
                    for (_,mut rates) in op_info.rates.iter_mut()
                    {
                        if rates.0 > agg_op_rate
                        {
                            if verbose { println!("Found {} instances for operator {}. Setting the aggregated true processing rate of the operator for epoch {} to the max value: {}.", inst, op_info.id, epoch, agg_op_rate); }
                            rates.0 = agg_op_rate;
                        }
                        epoch += 1;
                    }
                }
            },
            None => panic!("Operator not found in topology.")
        }
    }
}


/******* Timely methods *******/

/// Parses a Timely dataflow from a Timely log file and returns a topology
///
/// # Arguments
///
/// `file`: The path to the Timely log file
///
/// # Comments
///
/// * The Timely dataflow is extracted from Timely logs of the following form:
///
///   `Operates(OperatesEvent { id: OperatorId, addr: [WorkerId, X, LocalOperatorId], name: OperatorName }))`
///
///   `Channels(ChannelsEvent { id: ChannelId, scope_addr: [Y, W], source: (LocalOperatorId, Z), target: (LocalOperatorId, P) }))`
pub fn create_timely_topology(file: &Path) -> Topology
{
    let mut topology = Topology::new();
    let file = File::open(&file).expect("Cannot open topology file.");
    let reader = BufReader::new(file);

    let op_re = Regex::new(r####"Operates\(OperatesEvent\s+\{\s+id:\s+(\d+),\s+addr:\s\[(\d+),.+,\s(\d+)\],\sname:\s"(\s*(\w)+\s*)+""####).unwrap();
    let ch_re = Regex::new(r"Channels\(ChannelsEvent\s\{\sid:\s(\d+),\s.+source:\s\((\d+),.+target:\s\((\d+).+").unwrap();

    let mut operators = HashMap::new();
    let mut edges = HashSet::new();
    let mut op_id_mapping = HashMap::new();

    // Number of instances for each operator
    let mut instances = usize::max_value();

    for line in reader.lines()
    {
        let line = line.unwrap();
        if line.trim().len() == 0 || line.chars().next()==Some('#') {continue;}
        for cap in op_re.captures_iter(&line[..])
        { // println!("Unique operator id: {}, Worker id: {} Operator id: {} Name: {}", &cap[1], &cap[2], &cap[3], &cap[4]);
            op_id_mapping.entry(cap[3].to_string()).or_insert(cap[1].to_string());
            operators.entry(cap[1].to_string())
                    .or_insert_with(|| (cap[4].to_string(),Vec::new()))
                    .1.push(cap[2].to_string());
            continue;
        }
        for cap in ch_re.captures_iter(&line[..])
        { // println!("Channel id: {} Source operator id: {} Target operator id: {}", &cap[1], &cap[2], &cap[3]);
            edges.insert((cap[1].to_string(),cap[2].to_string(),cap[3].to_string()));
        }
    }
    for (op_id,(name,workers)) in operators.drain()
    {
        match instances == usize::max_value()
        {
            false => assert_eq!(instances,workers.len()),	// All operators in Timely have the same number of instances
            true => instances = workers.len()
        }
        let _ = topology.add_unique_node(&op_id[..],&name[..],workers.len() as u32);
    }
    for (c,s,t) in edges.drain()
    { // Map local operator ids to global ones
        let s_op_uid = op_id_mapping.get(&s).expect("No unique id found for source operator.");
        let t_op_uid = op_id_mapping.get(&t).expect("No unique id found for target operator.");
        let &(n1,_) = topology.dictionary.get(&s_op_uid[..]).expect("Source operator was not found in dictionary.");
        let &(n2,_) = topology.dictionary.get(&t_op_uid[..]).expect("Target operator was not found in dictionary.");
        topology.add_unique_edge(n1,n2);
        topology.channel_dictionary.insert(c,(s_op_uid.clone(),t_op_uid.clone()));
    }
    topology
}

/// Parses rates of Timely operators from a CSV file and stores them in a topology
///
/// # Arguments
///
/// `file`: The path to the CSV file
///
/// `topology`: The topology struct to store the rate information
///
/// `source_rates_file`: The path to an optional CSV file containing true output rates of source operators
///
/// `verbose`: A boolean flag to print further information to standard output
///
/// # Comments
///
/// * Each line in `file` contains the true and observed rates of an operator instance aggregated per time window. It has the form:
//
///   `window(Rates(worker_id,operator_id,true_processing_rate,true_output_rate,observed_processing_rate,observed_output_rate))`
///
/// * Each line in `source_rates_file` contains the true output rates of source operators. It has the form:
//
///   `operator_id,true_output_rate_per_instance`
///
/// * The true output rates in `source_rates_file` overwrite the respective rates from `file` (if any)
pub fn read_timely_rates(filepath: &Path, topology: &mut Topology, source_rates_filepath: Option<&Path>, verbose: bool)
{
    let file = File::open(&filepath).expect("Cannot open rates file.");
    let reader = BufReader::new(file);
    let mut logs: HashMap<OperatorId,HashMap<WorkerId,Vec<Log>>> = HashMap::new();

    let rates_re = Regex::new(r"replayed converted:\s+(\d+)\s+\(Rates\(Op\((\d+),\s+(\d+)\)\),\s+\((\d+\.*\d+),\s+(\d+\.*\d+),\s+(\d+\.*\d+),\s+(\d+\.*\d+)\)\)").unwrap();

    for line in reader.lines()
    {
        let line = line.unwrap();
        if line.trim().len() == 0 || line.chars().next()==Some('#') {continue;}
        for cap in rates_re.captures_iter(&line[..])
        { // println!("Timestamp: {}, Worker id: {}, Operator id: {} Rates: {}, {}, {}, {}",&cap[1],&cap[2],&cap[3],&cap[4],&cap[5],&cap[6],&cap[7]);
            let operator_slot = logs.entry(cap[3].to_string()).or_insert_with(|| HashMap::new());
            let instance_slot = operator_slot.entry(cap[2].to_string()).or_insert_with(|| Vec::new());
            instance_slot.push((u64::from_str(&cap[1]).expect("Cannot extract epoch timestamp."),
                                f64::from_str(&cap[4]).expect("Cannot extract true processing rate."),
                                f64::from_str(&cap[5]).expect("Cannot extract true output rate."),
                                f64::from_str(&cap[6]).expect("Cannot extract observed processing rate."),
                                f64::from_str(&cap[7]).expect("Cannot extract observed output rate."))
            			);
        }
    }
    // Aggregate rate per logical operator
    for (op_id,instance_logs) in logs.iter_mut()
    {
        let mut op_logs: HashMap<Epoch,Rates> = HashMap::new();
        for (_,mut logs) in instance_logs.iter_mut()
        {
            logs.sort_by(|a:&Log,b:&Log| a.0.cmp(&b.0));
            let mut epoch = 0;
            for &(_,true_proc_rate,true_out_rate,obs_proc_rate,obs_out_rate) in logs.iter()
            {
                let op_log = op_logs.entry(epoch).or_insert_with(|| Rates::default());
                op_log.0 += true_proc_rate;
                op_log.1 += true_out_rate;
                op_log.2 += obs_proc_rate;
                op_log.3 += obs_out_rate;
                epoch += 1;
            }
        }
    // Update topology with aggregated logs
    let &(idx,_) = topology.dictionary.get(op_id).expect("Operator not found in dictionary.");
    match topology.logical_graph.node_weight_mut(idx)
    {
        Some(op_info) => op_info.rates = op_logs,
        None => panic!("Operator not found in topology.")
    }
    }
    // Set true output source rates explicitly
    match source_rates_filepath
    {
        Some(fp) =>
        {
            let source_rates = parse_operator_rates(fp,verbose);
            set_true_output_rates(topology,source_rates,verbose);
        },
        None => {}
    }
}


/******* Other methods *******/

/// Sets the true output rates of operators in a topology
///
/// # Arguments
///
/// `topology`: The topology struct to update
///
/// `operator_rates`: A vector of (`OperatorId`,`Rate`) pairs, where
/// `OperatorId` is the id of an operator and
/// `Rate` is its true output rate
///
/// `verbose`: A boolean flag to print further information to standard output
///
/// # Comments
///
/// * This function is used to explicitly set the true output rates of the source operators
///
/// * If there are true output rates for `OperatorId` that correspond to multiple epochs, all of them are set to the given `Rate` value
///
/// * The function can be used with `Rate = -1f64` to set the true output rate of a source operator with id `OperatorId` equal to its observed output rate, as stored in `Topology`
fn set_true_output_rates(topology: &mut Topology, operator_rates: Vec<(OperatorId,Rate)>, verbose: bool)
{
    for (ref op_id, ref op_rate) in operator_rates
    { // There are given source rates. Use these instead of the logged values
        let op_idx = topology.get_node_idx(op_id);
        println!("Source id: {}, {:?}", op_id, op_idx);
        match topology.logical_graph.node_weight_mut(op_idx)
        {
            Some(op_info) =>
            {
                if *op_rate == -1f64
                { // Set the true output rate of op_id equal to its observed output rate.
                  // For source operators, this indicates there is no backpressure to this source.
                    if verbose { println!("\nSetting the aggregated true output rate of the operator (per epoch) equal to the aggregated observed rate."); }
                    for (_,mut rates) in op_info.rates.iter_mut()
                    {
                        rates.1 = rates.3;
                    }
                }
                else
                { // Set the true output rates to the provided values
                    // TODO: What if there are no recorded rates?
                    let inst = op_info.instances;
                    let agg_op_rate = op_rate * inst as f64;
                    if verbose { println!("\nFound {} operator instances. Setting the aggregated true output rate of the operator (per epoch) to {}.", inst, agg_op_rate); }
                    for (_,mut rates) in op_info.rates.iter_mut()
                    {
                        rates.1 = agg_op_rate;
                    }
                }
            },
            None => panic!("Operator not found in topology.")
        }
    }
}

/// Parses operator rates from a CSV file into a vector
///
/// # Arguments
///
/// `file`: The path to the CSV file
///
/// `verbose`: A boolean flag to print further information to standard output
///
/// # Returns
///
/// A vector of `(OperatorId,Rate)` pairs, where
/// `OperatorId` is the id of an operator and
/// `Rate` is its (processing or output) rate
///
/// # Comments
///
/// * Each line in `file` has the form:
///
///   `operator_id,true_rate_per_instance`
///
/// * This function is used in Heron experiments to provide a max true processing rate per operator, and in Flink/Timely experiments to provide a fixed true output rate for source operators
fn parse_operator_rates(file: &Path, verbose: bool) -> Vec<(OperatorId,Rate)>
{
    let file = File::open(&file).expect("Cannot open operator rates file.");
    let reader = BufReader::new(file);
    let mut source_rates = Vec::new();
    for line in reader.lines()
    {
        let line = line.unwrap();
        if line.len() == 0 || line.chars().next() == Some('#') { continue; }
        let fields: Vec<&str> = line.split(',').map(|x| x.trim()).collect();
        let op_id = fields[0].to_string();
        let op_rate = Rate::from_str(fields[1]).expect("Cannot parse operator rate.");
        if verbose { println!("Found operator {} with rate {} per instance",&op_id,op_rate); }
        source_rates.push((op_id,op_rate));
    }
    source_rates
}

/// Sets the true output rates of source operators to the values given in a CSV file
///
/// # Arguments
///
/// `file`: The path to the CSV file
///
/// `topology`: The topology struct to update
///
/// `verbose`: A boolean flag to print further information to standard output
///
/// # Comments
///
/// * Each line in `file` has the form:
///
///   `source_operator_id,true_output_rate_per_instance`
pub fn set_source_rates(file: &Path, topology: &mut Topology, verbose: bool)
{
    // TODO (john): Check if given operator IDs correspond to source operators
    let source_rates = parse_operator_rates(file,verbose);
    set_true_output_rates(topology,source_rates,verbose);
}
