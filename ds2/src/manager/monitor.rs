// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate notify;

use self::notify::{RecommendedWatcher, Watcher, RecursiveMode, DebouncedEvent};
use std::sync::mpsc::channel;
use std::time::Duration;
use std::ffi::OsStr;
use std::path::Path;
use std::collections::{HashMap};
use std::str::FromStr;

use dataflow::Epoch;
use dataflow::topology::Topology;
use dataflow::parse::*;

/// Monitors a directory periodically and sends a notification when a file of interest has been created
///
/// # Arguments
///
/// `path`: The directory to monitor
///
/// `file_extension`: The file extension
///
/// `polling_period_secs`: The polling period in seconds
pub fn monitor_flink_repo(path: &Path, topology: &mut Topology, file_extension: &str, polling_period_secs: u32, number_of_files: u32) -> notify::Result<()> {
    
    // Local state
    let mut epoch_files = HashMap::new();

    // Create a channel to receive the events.
    let (tx, rx) = channel();

    // Create a watcher
    let mut watcher: RecommendedWatcher = try!(Watcher::new(tx, Duration::from_secs(polling_period_secs as u64)));

    // Path to be watched
    try!(watcher.watch(path, RecursiveMode::Recursive));
    let os_str_ext = OsStr::new(file_extension);
    loop {
        match rx.recv() {
            Ok(DebouncedEvent::Create(p)) => {
                if let Some(ext) = p.extension() {
                    if ext == os_str_ext {
                        //println!("{:?}", p);
                        let filepath = p.to_str().expect("Error converting rates filepath.");
                        let idx = filepath.rfind('-').expect("Error extracting epoch number from rates file.");
                        let epoch = &filepath[idx+1..]; 
                        let mut slot = epoch_files.entry(epoch.to_string()).or_insert(0u32);
                        *slot += 1;

                        // Update topology with additional rate information
                        update_flink_rates(p.as_path(),topology,Epoch::from_str(epoch).expect("Error converting epoch number"));

                        if *slot == number_of_files 
                        {// We have all logs for this epoch, so return
                            return Ok(());
                        }
                    }
                }
            },
            Err(e) => println!("Monitoring error: {:?}", e),
            _ => {}
        }
    }
}