// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate config;
extern crate ds2;
extern crate notify;

use self::notify::{RecommendedWatcher, Watcher, RecursiveMode, DebouncedEvent};
use std::sync::mpsc::channel;
use std::time::Duration;
use std::ffi::{OsStr};
use std::collections::{HashMap};
use std::str::FromStr;
use std::f64;
use std::path::{PathBuf};
use config::{Config, ConfigError, File};
use std::process::Command;

use ds2::dataflow::{Epoch, OperatorId, OperatorInstances, parse::*};
use ds2::policy::scaling::*;

/// Formats a dataflow configuration for Flink scripts
fn format_flink_configuration(conf: &Vec<(OperatorId,OperatorInstances)>) -> String
{
    let mut flink_conf = String::from("");
    for pair in conf.iter()
    { // Format dataflow configuration for Flink's script
        flink_conf.push_str(format!("{},{}#", pair.0, pair.1).as_str());
    }
    flink_conf
}

/// Decides whether the change in the dataflow configuration is 'significant'
///
/// # Comments
///
/// * Operators in `before` and `after` are expected to be in the same order
fn consider_change(before: &Vec<(OperatorId,OperatorInstances)>, after: &Vec<(OperatorId,OperatorInstances)>) -> bool
{
    assert_eq!(before.len(),after.len());
    for i in 0..before.len()
    {
        let i_before = before[i].1;
        let i_after = after[i].1;
        if (i_before < i_after) || (i_before-1 > i_after) { return true; }
    }
    return false;
}

/// Configures and runs a new scaling manager
///
/// # Comments
///
/// * Use `config/ds2.toml` to configure the scaling manager
pub fn main() -> notify::Result<()>
{
    // Includes paths to necessary files, scripts, the metrics repository, etc. as well as the scaling manager parameters
    let mut conf = Config::new();
    match conf.merge(File::with_name("config/ds2.toml")) {
        Err(e) => panic!("Error parsing config file: {:?}", e),
        _ => {}
    };

    eprintln!("\nReading configuration parameters from 'config/ds2.toml'...\n");

    /******* System ID *******/

    let system = match conf.get::<String>("system.id")
    {
        Ok(sys) => { eprintln!("System id: {}",sys); sys.to_lowercase() },
        Err(e) => panic!("Error parsing system id parameter: {:?}", e),
    };

    /******* Paths *******/

    let topology_path = match conf.get::<PathBuf>("paths.topology")
    {
        Ok(topo) => { eprintln!("Topology file: {:?}",topo); topo },
        Err(e) => panic!("Error parsing topology parameter: {:?}", e),
    };

    let metrics_repo_path = match conf.get::<PathBuf>("paths.metrics_repo")
    {
        Ok(repo) => { eprintln!("Metrics repo: {:?}",repo); repo },
        Err(e) => panic!("Error parsing metrics repository parameter: {:?}", e),
    };

    let source_rates_path = match conf.get::<PathBuf>("paths.source_rates")
    {
        Ok(source_rates) => { eprintln!("Source rates file: {:?}",source_rates); source_rates },
        Err(e) => panic!("Error parsing source rates parameter: {:?}", e),
    };

    let reconfiguration_script_path = match conf.get::<PathBuf>("paths.reconfiguration_script")
    {
        Ok(script) => { println!("Reconfiguration script: {:?}",script); script },
        Err(e) => panic!("Error parsing reconfiguration script parameter: {:?}", e),
    };

    let start_script_path = match conf.get::<PathBuf>("paths.start_script")
    {
        Ok(script) => { eprintln!("Start script: {:?}",script); script },
        Err(e) => panic!("Error parsing start/stop script parameter: {:?}", e),
    };

    let file_extension = match conf.get::<String>("paths.file_extension")
    {
        Ok(file_extension) => { eprintln!("Log file extension: '{}'",file_extension); file_extension } ,
        Err(e) => panic!("Error parsing file extension parameter: {:?}", e),
    };

    /******* Scaling Manager parameters *******/

    let policy_interval = match conf.get::<u32>("scaling_manager.policy_interval")
    {
        Ok(policy_interval) => { eprintln!("Policy interval (secs): {}",policy_interval); policy_interval },
        Err(e) => panic!("Error parsing policy interval parameter: {:?}", e),
    };

    let activation_time = match conf.get::<u32>("scaling_manager.activation_time")
    {
        Ok(activation_time) => { eprintln!("Activation time (epochs): {}",activation_time); activation_time },
        Err(ConfigError::NotFound(_)) => { eprintln!("Activation time: 1 (default)"); 1 }, // Parameter is not specified, use the default value instead
        Err(e) => panic!("Error parsing activation time parameter: {:?}", e),
    };

    let warm_up_time = match conf.get::<u32>("scaling_manager.warm_up_time")
    {
        Ok(warm_up_time) => { eprintln!("Warmup time (epochs): {}",warm_up_time); warm_up_time },
        Err(e) => panic!("Error parsing warm up time parameter: {:?}", e),
    };

    let target_rate_ratio = match conf.get::<f64>("scaling_manager.target_rate_ratio")
    {
        Ok(target_rate_ratio) => { eprintln!("Target rate ratio: {}",target_rate_ratio); target_rate_ratio },
        Err(ConfigError::NotFound(_)) => { eprintln!("Target rate ratio: f64::MAX (default)"); f64::MAX },   // Parameter is not specified, use the default value instead
        Err(e) => panic!("Error parsing target rate ratio parameter: {:?}", e),
    };

    let scaling_factor = match conf.get::<f64>("scaling_manager.scaling_factor")
    {
        Ok(scaling_factor) => { eprintln!("Scaling factor: {}",scaling_factor); scaling_factor },
        Err(ConfigError::NotFound(_)) => { eprintln!("Scaling factor: 0 (default)"); 0.0 },   // Parameter is not specified, use the default value instead
        Err(e) => panic!("Error parsing scaling factor parameter: {:?}", e),
    };

    eprintln!("\nDone.\n");

    // Scaling manager's state
    let mut epoch_files = HashMap::new();
    let mut epochs_since_reconfiguration = 0u32;
    let mut evaluations = 0u32;
    // Create a channel to receive the events.
    let (tx, rx) = channel();
    // Create a watcher
    let mut watcher: RecommendedWatcher = try!(Watcher::new(tx.clone(), Duration::from_secs(policy_interval as u64)));
    // Path to be monitored
    try!(watcher.watch(metrics_repo_path.as_path(), RecursiveMode::Recursive));
    let os_str_ext = OsStr::new(file_extension.as_str());

    match system.as_str()
    {
        "flink" =>
        {
            // Step 0: Load initial dataflow topology and initialize local state
            eprint!("Reading initial dataflow configuration...");
            let mut topo = create_flink_topology(topology_path.as_path());
            eprintln!("Done.");
            let mut conf = topo.get_configuration(false);
            conf.sort();
            let mut num_instances: OperatorInstances = conf.iter().map(|(_,c)| c).sum();
            eprintln!("Loaded initial physical topology with {} operator instances.", num_instances);
            let script_argument = format_flink_configuration(&conf);
            // Step 1: Start Flink job
            let cmd_out = Command::new(start_script_path.as_os_str())
                                        .arg(script_argument)
                                        .output()
                                        .expect("Failed to run start script.");
            // Get Flink job id
            let mut job_id = match String::from_utf8(cmd_out.stdout)
            { // Capture all information printed to stdout into a string
                Ok(s) =>
                {
                    let j_id = "JobID";
                    match s.rfind(j_id)
                    { // Upon succesful job submission, the script is expected to print a
                      // last line of the form 'JobID ID' containing the submitted job ID
                        Some(idx) => { s[idx+j_id.len()..].trim().to_string() },
                        None => panic!("Error (JobID not found). Output of '{:?}' script:\n {}", start_script_path, s)
                    }
                },
                Err(e) => panic!("Error retrieving the Flink job id: {:?}", e)
            };
            eprintln!("Submitted Flink job with id: {}", job_id);
            loop
            { // Step 2: Start monitoring metrics repository
                match rx.recv()
                {
                    Ok(DebouncedEvent::Create(p)) =>
                    {
                        if let Some(ext) = p.extension()
                        {
                            if ext == os_str_ext
                            { // A log file was created or updated
                                if !p.as_path().exists() { continue; }  // Ignore files created during policy evaluation
                                let filepath = p.file_stem().expect("Error extracting rates file name.").to_str().expect("Error converting rates filepath.");
                                let idx = filepath.rfind('-').expect("Error extracting epoch number from rates file.");
                                let epoch = &filepath[idx+1..];
                                let num_log_files =
                                {
                                    let mut slot = epoch_files.entry(epoch.to_string()).or_insert(0u32);
                                    *slot += 1;
                                    *slot
                                };
                                // Update topology with additional rate information
                                update_flink_rates(p.as_path(),
                                                    &mut topo,
                                                    Epoch::from_str(epoch).expect("Error converting epoch number."));

                                if num_log_files == num_instances
                                { // We have all logs for this epoch
                                    eprintln!("All log files for epoch {} have been found.",epoch);
                                    // Retrieve true output rates of source operators (if any)
                                    eprintln!("Checking source operator rates...");
                                    set_source_rates(source_rates_path.as_path(),&mut topo,false);
                                    eprintln!("Done.");
                                    let mut new_conf = match epochs_since_reconfiguration >= warm_up_time
                                    {
                                        true =>
                                        { // Step 3: Invoke scaling policy
                                            eprintln!("Invoking policy...");
                                            evaluations += 1;
                                            let nc = evaluate_scaling_policy_at_epoch(&mut topo,
                                                                                    target_rate_ratio,
                                                                                    scaling_factor,
                                                                                    Epoch::from_str(epoch).expect("Error converting epoch number."),
                                                                                    false);
                                            eprintln!("Estimated optimal configuration at epoch {}: {}",epoch,nc);
                                            let mut c = as_vec(&nc);
                                            c.sort();
                                            c
                                        },
                                        false =>
                                        {
                                            eprintln!("Skipping policy...");
                                            conf.clone()
                                        }
                                    };
                                    // TODO (john): Step 4: Check if final adjustment is needed (now done inside evaluate_scaling_policy())

                                    /*
                                    *   TODO (john): Keep track of the decision history for as many
                                    *   epochs as the activation time and use it to make better
                                    *   re-configurations
                                    */

                                    // Step 5: Call re-configuration script if needed and update
                                    // scaling manager's local state upon successful re-deployment
                                    if evaluations >= activation_time &&
                                       consider_change(&conf,&new_conf)
                                    {
                                        eprintln!("Reconfiguring...");
                                        let script_argument = format_flink_configuration(&new_conf);
                                        let cmd_out = Command::new(reconfiguration_script_path.as_os_str())
                                                                .arg(job_id.as_str())
                                                                .arg(script_argument)
                                                                .output()
                                                                .expect("Failed to run re-configuration script.");
                                        // Retrieve new job id
                                        job_id = match String::from_utf8(cmd_out.stdout)
                                                    { // Capture all information printed to stdout into a string
                                                        Ok(s) =>
                                                        {
                                                            let j_id = "JobID";
                                                            match s.rfind(j_id)
                                                            { // Upon succesful job submission, the script is expected to print a
                                                              // last line of the form 'JobID ID' containing the submitted job ID
                                                                Some(idx) => { s[idx+j_id.len()..].trim().to_string() },
                                                                None => panic!("Error (JobID not found). Output of '{:?}' script:\n {}", reconfiguration_script_path, s)
                                                            }
                                                        },
                                                        Err(e) => panic!("Error retrieving the new Flink job id: {:?}", e)
                                                    };
                                        eprintln!("Submitted reconfigured job with id: {:?}", job_id);
                                        // Update topology metadata with the new configuration
                                        topo.set_configuration(&new_conf);
                                        conf = new_conf;
                                        epochs_since_reconfiguration = 0;
                                        evaluations = 0;
                                        // Remove old rates files
                                        // TODO: remove only files in the repo
                                        let _ = Command::new("rm")
                                                    .arg("-r")
                                                    .arg(metrics_repo_path.to_str().unwrap())
                                                    .output()
                                                    .expect("Failed to remove log files.");
                                        // Create a new rates folder
                                        let _ = Command::new("mkdir")
                                                    .arg(metrics_repo_path.to_str().unwrap())
                                                    .output()
                                                    .expect("Failed to create new rates folder.");
                                    }
                                    else
                                    { // No re-configuration was issued
                                        epochs_since_reconfiguration += 1;
                                    }
                                    // Clear epoch information
                                    epoch_files.remove(epoch);
                                }
                            }
                        }
                    },
                    Err(e) => panic!("Monitoring error: {:?}", e),
                    _ => {}
                }
            }
        },
        "heron" =>
        {
            // TODO (john):
            panic!("Heron is not supported yet.");
        },
        "timely" =>
        {
            // TODO (john):
            panic!("Timely is not supported yet.");
        },
        _ =>
        {
            panic!("Unknown system {}",system);
        }
    };
}
