package ch.ethz.systems.strymon.ds2.flink.wordcount.sources;

import ch.ethz.systems.strymon.ds2.common.RandomSentenceGenerator;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * A rate-limited source function that generates sentences with an initial fixed rate
 * for a given period, then switches to another rate for a period, and finally returns
 * to its initial configuration.
 */
public class DynamicRateSourceFunction extends RichParallelSourceFunction<String> implements CheckpointedFunction {

    /** how many sentences to output per second for rate_1 **/
    private final int sentenceRate_1;

    /** how many sentences to output per second for rate_2 **/
    private final int sentenceRate_2;

    /** the time period for rate_1 in minutes **/
    private final int period_1;

    /** the time period for rate_2 in minutes **/
    private final int period_2;

    /** the length of each sentence (in chars) **/
    private final int sentenceSize;

    private final RandomSentenceGenerator generator;

    private volatile boolean running = true;

    public DynamicRateSourceFunction(int rate_1, int rate_2, int size, int time_1, int time_2) {
        sentenceRate_1 = rate_1;
        sentenceRate_2 = rate_2;
        period_1 = time_1;
        period_2 = time_2;
        generator = new RandomSentenceGenerator();
        sentenceSize = size;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        long period1StartTime = System.currentTimeMillis();
        // period_1
        while (running && (System.currentTimeMillis() - period1StartTime < period_1 * 60000)) {
            long emitStartTime = System.currentTimeMillis();
            for (int i = 0; i < sentenceRate_1; i++) {
                ctx.collect(generator.nextSentence(sentenceSize, 20));
            }
            // Sleep for the rest of timeslice if needed
            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < 1000) {
                Thread.sleep(1000 - emitTime);
            }
        }
        long period2StartTime = System.currentTimeMillis();
        // period_2
        while (running && (System.currentTimeMillis() - period2StartTime < period_2 * 60000)) {
            long emitStartTime = System.currentTimeMillis();
            for (int i = 0; i < sentenceRate_2; i++) {
                ctx.collect(generator.nextSentence(sentenceSize));
            }
            // Sleep for the rest of timeslice if needed
            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < 1000) {
                Thread.sleep(1000 - emitTime);
            }
        }
        period1StartTime = System.currentTimeMillis();
        // period_3
        while (running && (System.currentTimeMillis() - period1StartTime < period_1 * 60000)) {
            long emitStartTime = System.currentTimeMillis();
            for (int i = 0; i < sentenceRate_1; i++) {
                ctx.collect(generator.nextSentence(sentenceSize));
            }
            // Sleep for the rest of timeslice if needed
            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < 1000) {
                Thread.sleep(1000 - emitTime);
            }
        }

        ctx.close();
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}
