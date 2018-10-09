This folder contains template reconfiguration scripts for the Flink wordcount dataflow example.
You can use these script to run the example with DS2 or as templates to control your own Flink application with DS2.

- **start-wordcount.sh** submits the [StatefulWordcount](https://github.com/strymon-system/ds2/blob/master/flink-examples/src/main/java/ch/ethz/systems/strymon/ds2/flink/wordcount/StatefulWordCount.java) example to Flink. The script assumes you have a properly configured Apache Flink cluster up and running and that you have built the examples jar.

In order to run this script, make sure to point the `FLINK_BUILD_PATH` parameter to your flink installation and the `JAR_PATH` parameter to the examples jar.

Please refer to the [Apache Flink documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.4/ops/config.html) for details on how to configure your cluster.
