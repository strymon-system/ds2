#!/bin/bash

### ### ###  		   ### ### ###

### ### ### INITIALIZATION ### ### ###

### ### ###  		   ### ### ###

### paths configuration ###
FLINK_BUILD_PATH="/path/to/flink-1.4.1-instrumented/flink-1.4.1/build-target/"
FLINK=$FLINK_BUILD_PATH$"bin/flink"
JAR_PATH="/path/to/flink-examples-1.0-SNAPSHOT-jar-with-dependencies.jar"

### dataflow configuration ###
QUERY_CLASS="ch.ethz.systems.strymon.ds2.flink.wordcount.StatefulWordCount"
SOURCE_NAME="Source: Custom Source"
MAP_NAME="Splitter FlatMap"
COUNT_NAME="Count -> Latency Sink"

### operators and parallelism
if [ "$1" == "" ]; then
    echo "Please provide operators and their initial parallelism"
    exit 1
fi

### parse operator pairs
IFS='#' read -r -a array <<< "$1"
for element in "${array[@]}"
do
    IFS=',' read -r -a parallelism <<< "$element"
        ## search for SOURCE_NAME
    if [ "${parallelism[0]}" == "$SOURCE_NAME" ]; then
        echo "Source parallelism: ${parallelism[@]: -1:1}"
        P_SOURCE="${parallelism[@]: -1:1}"
    fi
    ## search for FlatMap
    if [ "${parallelism[0]}" == "$MAP_NAME" ]; then
        echo "FlatMap parallelism: ${parallelism[@]: -1:1}"
        P1="${parallelism[@]: -1:1}"
    fi
    ## search for Count
    if [ "${parallelism[0]}" == "$COUNT_NAME" ]; then
        echo "Count parallelism: ${parallelism[@]: -1:1}"
        P2="${parallelism[@]: -1:1}"
    fi
done

nohup $FLINK run -d --class $QUERY_CLASS $JAR_PATH --p1 $P_SOURCE --p2 $P1 --p3 $P2 & > job.out
