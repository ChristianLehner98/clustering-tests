package example;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.FeedbackBuilder;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.LoopContext;
import org.apache.flink.streaming.api.functions.windowing.WindowLoopFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.tasks.progress.FixpointIterationTermination;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

/**
 * This is a totally useless demonstration program that shows some behaviour that is unintuitive to me
 *
 * 1. The onTemination function is called for every new Watermark, even if no Window terminates at hat timestamp
 *
 * 2. Windows that don't have a Watermark for their exact end seem to be ignored completely
 *
 * This would mean that the WindowSize and the sending of Watermarks from the source have to be coordinated
 * to each other, which seems not exactly like desired behavior
 */

public class OnTerminationExample {

    public static void main(String args[]) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //parallelism 1 here only for testing purposes
        env.setParallelism(1);

        DataStream<Long> input = env.addSource(new OTExampleSource());

        KeyedStream<Long, Long> keyedInput = input.keyBy(new KeySelector<Long, Long>() {
            @Override
            public Long getKey(Long element) throws Exception {
                return element;
            }
        });

        WindowedStream<Long, Long, TimeWindow> windowedInput = keyedInput.timeWindow(Time.milliseconds(1000));


        DataStream<Long> results = windowedInput.iterateSync(new OTExampleWindowLoopFunction(),
            new FixpointIterationTermination(), new FeedbackBuilder<Long, Long>() {
                @Override
                public KeyedStream<Long, Long> feedback(DataStream<Long> dataStream) {
                    return dataStream.keyBy(new KeySelector<Long, Long>() {
                        @Override
                        public Long getKey(Long element) throws Exception {
                            return element;
                        }
                    });
                }
            }, BasicTypeInfo.LONG_TYPE_INFO);


        env.execute();


    }

    private static class OTExampleSource extends RichSourceFunction<Long> {


        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            for(long count = 0; count < 100; count++) {
                ctx.collectWithTimestamp(count + 1, count * 400);
                ctx.emitWatermark(new Watermark(count * 400 - 1));
                Thread.sleep(400);
            }
        }

        @Override
        public void cancel() {

        }
    }

    private static class OTExampleWindowLoopFunction implements WindowLoopFunction<Long, Long, Long, Long, Long, TimeWindow> {

        @Override
        public void entry(LoopContext<Long> loopContext, Iterable<Long> iterable,
            Collector<Either<Long, Long>> collector) throws Exception {

            //Windows that don't have an exact Watermark for their endpoint are ignored completely
            System.out.println("ENTRY: " + iterable.iterator().next());

        }

        @Override
        public void step(LoopContext<Long> loopContext, Iterable<Long> iterable,
            Collector<Either<Long, Long>> collector) throws Exception {
            //not necessary to see the point of the example
        }

        @Override
        public void onTermination(LoopContext<Long> loopContext, Collector<Either<Long, Long>> collector)
            throws Exception {

            //the function is called wheenever a new Watermark is received instead of at the end of a window
            System.out.println("ON_TERMINATION :: " + loopContext.getContext());

        }
    }
}
