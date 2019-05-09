package lostmessagestest;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import lostmessagestest.messages.AddNeighbor;
import lostmessagestest.messages.TestMessage;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
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

public class Main {

    private static final List<Tuple3<Long, List<Long>, Long>> sampleStream = Lists.newArrayList(

        // vertex1 - vertex2 - timestamp
        new Tuple3<>(1L, Lists.newArrayList(), 400L),
        new Tuple3<>(2L, Lists.newArrayList(), 600L),
        new Tuple3<>(3L, Lists.newArrayList(1L, 2L), 800L),
        new Tuple3<>(4L, Lists.newArrayList(), 1000L),
        new Tuple3<>(5L, Lists.newArrayList(4L), 1500L),
        new Tuple3<>(6L, Lists.newArrayList(4L, 5L), 2000L),
        new Tuple3<>(7L, Lists.newArrayList(1L, 2L, 3L), 2500L),
        new Tuple3<>(8L, Lists.newArrayList(1L, 3L, 5L, 7L), 3000L),
        new Tuple3<>(9L, Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L), 3500L),
        new Tuple3<>(10L, Lists.newArrayList(), 4000L),
        new Tuple3<>(11L, Lists.newArrayList(10L), 4500L),
        new Tuple3<>(12L, Lists.newArrayList(10L), 5000L)

    );

    public static void main(String args[]) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //parallelism 1 only for testing purposes
        env.setParallelism(4);

        DataStream<Tuple2<Long, List<Long>>> input = env.addSource(new CCSampleSrc());
        KeyedStream<Tuple2<Long, List<Long>>, Long> keyedInput = input
            .keyBy(new KeySelector<Tuple2<Long, List<Long>>, Long>() {
                @Override
                public Long getKey(Tuple2<Long, List<Long>> value) throws Exception {
                    return value.f0;
                }
            });

//        keyedInput.print();

        WindowedStream<Tuple2<Long, List<Long>>, Long, TimeWindow> windowedInput = keyedInput
            .timeWindow(Time.milliseconds(1000));

        DataStream<String> results = windowedInput.iterateSync(
            new TestWindowLoopFunction(), new FixpointIterationTermination(),
            new FeedbackBuilder<Tuple3<Long, Long, TestMessage>, Long>() {
                @Override
                public KeyedStream<Tuple3<Long, Long, TestMessage>, Long> feedback(
                    DataStream<Tuple3<Long, Long, TestMessage>> input) {
                    return input.keyBy(new KeySelector<Tuple3<Long, Long, TestMessage>, Long>() {
                        @Override
                        public Long getKey(Tuple3<Long, Long, TestMessage> value) throws Exception {
                            return value.f1;
                        }
                    });
                }
            }, new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.of(
                new TypeHint<TestMessage>() {
                })));

        results.print();

        env.execute();
    }

    private static class CCSampleSrc extends RichSourceFunction<Tuple2<Long, List<Long>>> {

        @Override
        public void run(SourceContext<Tuple2<Long, List<Long>>> ctx) throws Exception {
            long curTime = -1;
            for (Tuple3<Long, List<Long>, Long> next : sampleStream) {
                if (next.f2 - curTime > 0) {
                    Thread.sleep(next.f2 - curTime);
                }
                ctx.collectWithTimestamp(new Tuple2<>(next.f0, next.f1), next.f2);

                if (curTime == -1) {
                    curTime = next.f2;
                }
                if (curTime < next.f2) {
                    curTime = next.f2;
                    ctx.emitWatermark(new Watermark(curTime - 1));

                }
            }
        }

        @Override
        public void cancel() {
        }
    }

    private static class TestWindowLoopFunction implements
        WindowLoopFunction<Tuple2<Long, List<Long>>, Tuple3<Long, Long, TestMessage>, String, Tuple3<Long, Long, TestMessage>, Long, TimeWindow> {

        @Override
        public void entry(LoopContext<Long> ctx, Iterable<Tuple2<Long, List<Long>>> iterable,
            Collector<Either<Tuple3<Long, Long, TestMessage>, String>> out) throws Exception {

            Tuple2<Long, List<Long>> next = iterable.iterator().next();

            for (long neighbor : next.f1) {
                out.collect(new Either.Left<>(
                    new Tuple3<>(ctx.getKey(), neighbor, new AddNeighbor())));
                out.collect(new Either.Right<>("edge: (" + ctx.getKey() + "," + neighbor + ")"));
            }

        }

        @Override
        public void step(LoopContext<Long> ctx, Iterable<Tuple3<Long, Long, TestMessage>> iterable,
            Collector<Either<Tuple3<Long, Long, TestMessage>, String>> out) throws Exception {

            Iterator<Tuple3<Long, Long, TestMessage>> iterator = iterable.iterator();
            Tuple3<Long, Long, TestMessage> next = iterator.next();

            while (true) {
                if (next.f2 instanceof AddNeighbor) {
                    out.collect(new Either.Right<>("edge: (" + ctx.getKey() + "," + next.f0 + ")"));
                }
                if (iterator.hasNext()) {
                    next = iterator.next();
                } else {
                    break;
                }
            }

        }

        @Override
        public void onTermination(LoopContext<Long> ctx,
            Collector<Either<Tuple3<Long, Long, TestMessage>, String>> out) throws Exception {

        }
    }

}
