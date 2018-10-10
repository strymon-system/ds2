# DS2: fast, accurate, automatic scaling decisions for distributed streaming dataflows.

DS2 is a low-latency controller for dynamic scaling of streaming analytics applications. It can accurately estimate parallelism for all dataflow operators within a _single_ scaling decision, and operates _reactively_ online. DS2 bases scaling decisions on real-time performance traces which it collects through lightweight system-level instrumentation.

This repository contains the following DS2 components:
* The **Scaling policy** implements the scaling model and estimates operator parallelism using metrics collected by the reference system instrumentation.
* The **Scaling manager** periodically invokes the policy when metrics are available and sends scaling commands to the reference stream processor.
* The **Apache Flink 1.4.1 instrumentation patch** contains the necessary instrumentation for Flink to be integrated with DS2.

DS2 can be integrated with any dataflow stream processor as long as it can provide the instrumentation and metrics that its scaling policy requires. For more details on required metrics and integration, please see the [OSDI'18 paper](https://www.usenix.org/system/files/osdi18-kalavri.pdf).

## Building and executing DS2
These instructions assume a Unix-like system.

1. DS2 is mainly developed in [Rust](https://www.rust-lang.org). You can install Rust by following [these instructions](https://www.rust-lang.org/downloads.html).

2. Compile the code (dependencies will be fetched automatically):
    ```bash
    $ cd ds2/
    $ cargo build --release --all
    ```

3. Set-up the DS2 configuration parameters in the [ds2.toml](https://github.com/strymon-system/ds2/blob/master/ds2/config/ds2.toml) file.

4. Run the scaling manager:
    ```bash
    $ cargo run --release --bin monitor
    ```

## License

DS2 is primarily distributed under the terms of both the MIT license and the Apache License (Version 2.0).
See LICENSE-APACHE, and LICENSE-MIT for details.
