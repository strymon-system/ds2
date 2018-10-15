# DS2 example input files 

The following folders contain examples of topology and rate files for Flink, Heron, and Timely:

* Folder `flink_wordcount_rates` is an example metrics repository containing rate files per operator instance and epoch, as required by the DS2 scaling manager. See also: [ds2.toml](https://github.com/strymon-system/ds2/blob/master/controller/config/ds2.toml)

* Folder `offline` contains examples of a topology and a rate file for a Flink wordcount dataflow, which can be used for offline DS2 execution. See also: [README.md](https://github.com/strymon-system/ds2/blob/master/README.md)

* Folder `topology` contains examples of topology files for Flink, Heron, and Timely

* Folder `source_rates` contains examples of source rate files, which can be used to explicitly set the true output rate of source operators in a dataflow
