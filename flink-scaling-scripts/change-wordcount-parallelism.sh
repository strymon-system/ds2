#!/bin/bash

### ### ###  		   ### ### ###

### ### ### INITIALIZATION ### ### ###

### ### ###  		   ### ### ###

### paths configuration ###
FLINK_BUILD_PATH="/path/to/flink-1.4.1-instrumented/flink-1.4.1/build-target/"
FLINK=$FLINK_BUILD_PATH$"bin/flink"
JAR_PATH="/path/to/flink-examples-1.0-SNAPSHOT-jar-with-dependencies.jar"
readonly SAVEPOINT_PATH="/path/to/savepoints/"

### dataflow configuration ###
QUERY_CLASS="ch.ethz.systems.strymon.ds2.flink.wordcount.StatefulWordCount"
SOURCE_NAME="Source: Custom Source"
MAP_NAME="Splitter FlatMap"
COUNT_NAME="Count -> Latency Sink"

### jobId
if [ "$1" == "" ]; then
    echo "Please provide the Job ID as the first argument"
    exit 1
fi
JOB_ID=$1

### operators and parallelism
if [ "$2" == "" ]; then
    echo "Please provide operators and their initial parallelism as the second argument"
    exit 1
fi


### parse operator pairs
IFS='#' read -r -a array <<< "$2"
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

#echo Canceling job with savepoint
savepointPathStr=$($FLINK cancel -s $SAVEPOINT_PATH $JOB_ID)

savepointFile=$(echo $savepointPathStr| rev | cut -d'/' -f 1 | rev)
x=$(echo $savepointFile |tr -d '.')
x=$(echo $x |tr -d '\n')


nohup $FLINK run -d -s $SAVEPOINT_PATH$x --class $QUERY_CLASS $JAR_PATH --p1 $P_SOURCE --p2 $P1 --p3 $P2 & > job.out
