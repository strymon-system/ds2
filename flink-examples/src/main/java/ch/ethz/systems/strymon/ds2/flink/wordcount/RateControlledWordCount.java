package ch.ethz.systems.strymon.ds2.flink.wordcount;

import ch.ethz.systems.strymon.ds2.flink.wordcount.sources.RateControlledSourceFunction;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

public class RateControlledWordCount {
	private final static int SENTENCE_SIZE = 100;

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		if (!params.has("p1") || !params.has("p2") || !params.has("p3")
				|| !params.has("sentence-size")|| !params.has("source-rate")) {
			System.out.println("Use --p1 --p2 --p3 --sentence-size -- source-rate"
					+ "to specify the parallelism of the source, tokenizer, and count operators respectively,"
					+ "the sentence size, and the source output rate.");
			System.exit(-1);
		}

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.disableOperatorChaining();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		final DataStream<String> text = env.addSource(
				new RateControlledSourceFunction(
						params.getInt("source-rate", 80000),
						params.getInt("sentence-size", 100)))
					.setParallelism(params.getInt("p1", 1));

		// split up the lines in pairs (2-tuples) containing:
		// (word,1)
		DataStream<Tuple2<String, Integer>> counts = text
				.flatMap(new Tokenizer())
					.setParallelism(params.getInt("p2", 1))
				.keyBy(0)
				.sum(1)
					.setParallelism(params.getInt("p3", 1));
		// write to dummy sink
		counts.addSink(new SinkFunction<Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			public void invoke(Tuple2<String, Integer> value) {
				// nop
			}
		}).setParallelism(params.getInt("p3", 1));

		// execute program
		JobExecutionResult res = env.execute("Rate-controlled Streaming WordCount");
		System.err.println("Execution time: " + res.getNetRuntime());
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and splits
	 * it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}

}
