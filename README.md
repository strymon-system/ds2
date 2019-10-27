# DS2: fast, accurate, automatic scaling decisions for distributed streaming dataflows.

DS2 is a low-latency controller for dynamic scaling of streaming analytics applications. It can accurately estimate parallelism for all dataflow operators within a _single_ scaling decision, and operates _reactively_ online. DS2 bases scaling decisions on real-time performance traces which it collects through lightweight system-level instrumentation.

This repository contains the following DS2 components:
* The **Scaling policy** implements the scaling model and estimates operator parallelism using metrics collected by the reference system instrumentation.
* The **Scaling manager** periodically invokes the policy when metrics are available and sends scaling commands to the reference stream processor.
* The **Apache Flink 1.4.1 instrumentation patch** contains the necessary instrumentation for Flink to be integrated with DS2.

DS2 can be integrated with any dataflow stream processor as long as it can provide the instrumentation and metrics that its scaling policy requires. For more details on required metrics and integration, please see the [OSDI'18 paper](https://www.usenix.org/system/files/osdi18-kalavri.pdf).

## Building DS2
These instructions assume a Unix-like system.

1. DS2 is mainly developed in [Rust](https://www.rust-lang.org). You can install Rust by following [these instructions](https://www.rust-lang.org/tools/install).

2. Compile the code (dependencies will be fetched automatically):
    ```bash
    $ cd controller/
    $ cargo build --release --all
    ```

## Executing DS2 

There are two ways to execute DS2: _online_ and _offline_. To run DS2 online, follow the steps below:

1. Set up the DS2 configuration parameters in the [ds2.toml](https://github.com/strymon-system/ds2/blob/master/controller/config/ds2.toml) file

2. Go to `controller/` and start the scaling manager:

```bash
$ cargo run --release --bin manager
```

On success, the scaling manager starts monitoring the specified metrics repository and performs the following actions:

* Updates its state every time a new rate file is created by the instrumented system. The scaling manager expects _exactly one rate file per operator instance and time window (epoch)_ following the naming convention "some_name-epoch_number.file_extension". Example rate files generated with [this patch](https://github.com/strymon-system/ds2/tree/master/flink-instrumentation) for Flink can be found [here](https://github.com/strymon-system/ds2/tree/master/controller/examples/flink_wordcount_rates).
* Invokes the scaling policy periodically according to the particular configuration.
* Re-configures the streaming system automatically. To start and re-configure the running system, you must write two respective bash scripts, such as [these scripts](https://github.com/strymon-system/ds2/tree/master/flink-scaling-scripts) for Flink.

DS2 scaling policy can also be invoked offline on a collection of metrics generated during the execution of a dataflow. To do so, you need:

* A file containing the configuration of the executed dataflow.
* A log file with the collected operator rates for one or more policy intervals (epochs).

As an example, go to `controller/` and run:

```bash
$ cargo run --release --bin policy -- --topo examples/offline/flink_wordcount_topology.csv --rates examples/offline/flink_rates.log --system flink
```
This command evaluates the scaling policy on the `--topo` topology for each epoch included in the `--rates` file assuming Flink as the streaming system.

For more information about offline execution parameters, try `--help` as follows:

```bash
$ cargo run --release --bin policy -- --help
```

Example input files for both online and offline DS2 execution are provided [here](https://github.com/strymon-system/ds2/tree/master/controller/examples), along with details on their format.

## Documentation

The complete DS2 documentation can be found [here](http://strymon.systems.ethz.ch/ds2/ds2/index.html).

## License

DS2 is primarily distributed under the terms of both the MIT license and the Apache License (Version 2.0).
See LICENSE-APACHE, and LICENSE-MIT for details.
