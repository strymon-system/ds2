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
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.AuctionSourceFunction;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.PersonSourceFunction;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class Query8 {

    private static final Logger logger  = LoggerFactory.getLogger(Query8.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        // enable latency tracking
        //env.getConfig().setLatencyTrackingInterval(5000);

        final int auctionSrcRate = params.getInt("auction-srcRate", 50000);

        final int personSrcRate = params.getInt("person-srcRate", 30000);

        env.setParallelism(params.getInt("p-window", 1));

        DataStream<Person> persons = env.addSource(new PersonSourceFunction(personSrcRate))
                .name("Custom Source: Persons")
                .setParallelism(params.getInt("p-person-source", 1))
                .assignTimestampsAndWatermarks(new PersonTimestampAssigner());

        DataStream<Auction> auctions = env.addSource(new AuctionSourceFunction(auctionSrcRate))
                .name("Custom Source: Auctions")
                .setParallelism(params.getInt("p-auction-source", 1))
                .assignTimestampsAndWatermarks(new AuctionTimestampAssigner());

        // SELECT Rstream(P.id, P.name, A.reserve)
        // FROM Person [RANGE 1 HOUR] P, Auction [RANGE 1 HOUR] A
        // WHERE P.id = A.seller;
        DataStream<Tuple3<Long, String, Long>> joined =
                persons.join(auctions)
                .where(new KeySelector<Person, Long>() {
                    @Override
                    public Long getKey(Person p) {
                        return p.id;
                    }
                }).equalTo(new KeySelector<Auction, Long>() {
                            @Override
                            public Long getKey(Auction a) {
                                return a.seller;
                            }
                        })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new FlatJoinFunction<Person, Auction, Tuple3<Long, String, Long>>() {
                    @Override
                    public void join(Person p, Auction a, Collector<Tuple3<Long, String, Long>> out) {
                        out.collect(new Tuple3<>(p.id, p.name, a.reserve));
                    }
                });


        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        joined.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-window", 1));

        // execute program
        env.execute("Nexmark Query8");
    }

    private static final class PersonTimestampAssigner implements AssignerWithPeriodicWatermarks<Person> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Person element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.dateTime);
            return element.dateTime;
        }
    }

    private static final class AuctionTimestampAssigner implements AssignerWithPeriodicWatermarks<Auction> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Auction element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.dateTime);
            return element.dateTime;
        }
    }

}