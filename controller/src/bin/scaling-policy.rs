// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate clap;
extern crate ds2;

use std::path::Path;
use std::str::FromStr;
use std::f64;

use clap::{App, Arg};

use ds2::dataflow::parse::*;
use ds2::policy::scaling::*;

/// Invokes the scaling policy once with the given parameters
///
/// # Comments
///
/// * Policy parameters are given as command line arguments. Use --help
pub fn main() {

    let matches = App::new("DS2")
        .about("DS2 is an automatic scaling controller for distributed streaming dataflow systems.")
        .arg(Arg::with_name("topology")
            .help("The logical dataflow file")
            .long("topo")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("rates")
            .help("The operator rates file")
            .long("rates")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("system")
            .help("The system under test (flink, timely, heron)")
            .long("system")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("source-rates-file")
            .help("A CSV file with true output rates of source operator instances")
            .long("source-rates")
            .takes_value(true))
        .arg(Arg::with_name("operator-rates-file")
            .help("A CSV file with true processing rates of operator instances")
            .long("operator-rates")
            .takes_value(true))
        .arg(Arg::with_name("verbose")
            .help("Print detailed messages (for debugging)")
            .short("v")
            .long("verbose"))
        .arg(Arg::with_name("threshold")
            .help("The target rate ratio threshold")
            .long("threshold")
            .takes_value(true))
        .arg(Arg::with_name("factor")
            .help("A scaling adjustment factor (for deliberate overprovisioning)")
            .long("factor")
            .takes_value(true))
        .get_matches();

    let verbose = 1 == matches.occurrences_of("verbose");
    let rates_file = Path::new(matches.value_of("rates").expect("Cannot parse rates filepath."));
    let graph_file = Path::new(matches.value_of("topology").expect("Cannot parse topology filepath."));
    let system = matches.value_of("system").expect("Cannot parse system name.");
    let threshold = match matches.value_of("threshold")
    {
        Some(e) => f64::from_str(e).expect("Cannot parse threshold."),
        None => f64::MAX    // This will never trigger a configuration adjustment
    };
    if threshold < 1f64 { panic!{"Rate threshold cannot be below 1."}; }
    let factor = match matches.value_of("factor")
    {
        Some(e) => f64::from_str(e).expect("Cannot parse factor."),
        None => 0.0 // This will not over-provision
    };
    if factor < 0f64 { panic!{"Smoothing factor cannot be below zero."}; }
    let source_rates_filepath = match matches.value_of("source-rates-file")
    { // Used to explicitly set the source output rates as monitored outside the dataflow system
        Some(fp) => Some(Path::new(fp)),
        None => None // No true source rates are given, use system logs instead
    };
    let operator_rates_filepath = match matches.value_of("operator-rates-file")
    { // Used in Heron experiments to simulate the Dhallion wordcount experiment
        Some(fp) => Some(Path::new(fp)),
        None => None // No true processing rates are given, use system logs instead
    };

    match system
    {
        "flink" =>
        {
            let mut topo = create_flink_topology(&graph_file);
            read_flink_rates(&rates_file,&mut topo,source_rates_filepath,verbose);
            if verbose { topo.print(); }
            println!("{:?}",evaluate_scaling_policy(&mut topo,threshold,factor,verbose));
        },
        "heron" =>
        {
            let mut topo = create_heron_topology(&graph_file);
            read_heron_rates(&rates_file,&mut topo,source_rates_filepath,operator_rates_filepath,verbose);
            if verbose { topo.print(); }
            println!("{:?}",evaluate_scaling_policy(&mut topo,threshold,factor,verbose));
        },
        "timely" =>
        {
            let mut topo = create_timely_topology(&graph_file);
            if verbose { topo.print(); }
            read_timely_rates(&rates_file,&mut topo,source_rates_filepath,verbose);
            println!("{:?}",evaluate_scaling_policy(&mut topo,threshold,factor,verbose));
        },
        _ =>
        {
            panic!("Unknown system {}",system);
        }
    };
}
