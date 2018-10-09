/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.ethz.systems.strymon.ds2.flink.nexmark.queries;

import ch.ethz.systems.strymon.ds2.flink.nexmark.sinks.DummyLatencyCountingSink;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.BidSourceFunction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class Query5 {

    private static final Logger logger  = LoggerFactory.getLogger(Query5.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);

        final int srcRate = params.getInt("srcRate", 100000);

        DataStream<Bid> bids = env.addSource(new BidSourceFunction(srcRate))
                .setParallelism(params.getInt("p-bid-source", 1))
                .assignTimestampsAndWatermarks(new TimestampAssigner());

        // SELECT B1.auction, count(*) AS num
        // FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B1
        // GROUP BY B1.auction
        DataStream<Tuple2<Long, Long>> windowed = bids.keyBy(new KeySelector<Bid, Long>() {
            @Override
            public Long getKey(Bid bid) throws Exception {
                return bid.auction;
            }
        }).timeWindow(Time.minutes(60), Time.minutes(1))
                .aggregate(new CountBids())
                .name("Sliding Window")
                .setParallelism(params.getInt("p-window", 1));

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        windowed.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-window", 1));

        // execute program
        env.execute("Nexmark Query5");
    }

    private static final class TimestampAssigner implements AssignerWithPeriodicWatermarks<Bid> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Bid element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.dateTime);
            return element.dateTime;
        }
    }

    private static final class CountBids implements AggregateFunction<Bid, Long, Tuple2<Long, Long>> {

        private long auction = 0L;

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Bid value, Long accumulator) {
            auction = value.auction;
            return accumulator + 1;
        }

        @Override
        public Tuple2<Long, Long> getResult(Long accumulator) {
            return new Tuple2<>(auction, accumulator);
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}