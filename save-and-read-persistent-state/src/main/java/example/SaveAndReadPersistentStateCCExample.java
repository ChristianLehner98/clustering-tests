package example;


import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
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

public class SaveAndReadPersistentStateCCExample {


    private static final List<Tuple3<Long, Long, Long>> sampleStream = Lists.newArrayList(

        // vertex1 - vertex2 - timestamp
        new Tuple3<>(1L, 2L, 2000L),
        new Tuple3<>(2L, 3L, 3000L),
        new Tuple3<>(4L, 5L, 4000L),
        new Tuple3<>(1L, 3L, 5000L),
        new Tuple3<>(6L, 7L, 7500L),
        new Tuple3<>(3L, 7L, 10000L),
        new Tuple3<>(8L, 9L, 12500L),
        new Tuple3<>(9L, 10L, 15000L),
        new Tuple3<>(9L, 11L, 17500L),
        new Tuple3<>(6L, 12L, 20000L),
        new Tuple3<>(13L, 14L, 22500L),
        new Tuple3<>(15L, 16L, 25000L)

    );

    public static void main(String args[]) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //parallelism 1 only for testing purposes
        env.setParallelism(1);

        DataStream<Tuple2<Long, Long>> input = env.addSource(new CCSampleSrc()).flatMap(
            new FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                @Override
                public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Long>> out)
                    throws Exception {
                    out.collect(element);
                    out.collect(new Tuple2<>(element.f1, element.f0));
                }
            });
        KeyedStream<Tuple2<Long, Long>, Long> keyedInput = input
            .keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {
                @Override
                public Long getKey(Tuple2<Long, Long> value) throws Exception {
                    return value.f0;
                }
            });

//        keyedInput.print();

        WindowedStream<Tuple2<Long, Long>, Long, TimeWindow> windowedInput = keyedInput
            .timeWindow(Time.milliseconds(5000));

        DataStream<Tuple2<Long, Long>> results = windowedInput.iterateSync(
            new CCWindowLoopFunction(), new FixpointIterationTermination(),
            new FeedbackBuilder<Tuple2<Long, Long>, Long>() {

                @Override
                public KeyedStream<Tuple2<Long, Long>, Long> feedback(
                    DataStream<Tuple2<Long, Long>> input) {

                    return input.keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {
                        @Override
                        public Long getKey(Tuple2<Long, Long> value) throws Exception {
                            return value.f0;
                        }
                    });
                }
            }, new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO));

        results.print();

        env.execute();
    }

    private static class CCSampleSrc extends RichSourceFunction<Tuple2<Long, Long>> {

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
            long curTime = -1;
            for (Tuple3<Long, Long, Long> next : sampleStream) {
                if(next.f2 - curTime > 0) {
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

    private static class CCWindowLoopFunction implements
        WindowLoopFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>, Long, TimeWindow> {

        private final ListStateDescriptor<Long> listStateDesc =
            new ListStateDescriptor<>("neighbors", BasicTypeInfo.LONG_TYPE_INFO);
        private Map<List<Long>, Map<Long, List<Long>>> neighborsPerContext = new HashMap<>();
        private Map<List<Long>, Map<Long, Long>> componentsPerContext = new HashMap<>();
        private MapState<Long, List<Long>> persistentGraph = null;
        private MapState<Long, Long> persistentComponents = null;


        @Override
        public void entry(LoopContext<Long> loopContext, Iterable<Tuple2<Long, Long>> edges,
            Collector<Either<Tuple2<Long, Long>, Tuple2<Long, Long>>> out) throws Exception {


            initState(loopContext);

            //old data does not actually get loaded
            System.out.println("LOAD :: persistent graph: " + persistentGraph.entries() + "; persistent components: " + persistentComponents.entries());

            Map<Long, List<Long>> adjacencyList = neighborsPerContext.get(loopContext.getContext());
            if (adjacencyList == null) {
                //load persistent neighbors
                adjacencyList = new HashMap<>();
                for (Entry<Long, List<Long>> entry : persistentGraph.entries()) {
                    adjacencyList.put(entry.getKey(), entry.getValue());
                }
                neighborsPerContext.put(loopContext.getContext(), adjacencyList);
            }

            Map<Long, Long> components = componentsPerContext.get(loopContext.getContext());
            if (components == null) {
                //load persistent components
                components = new HashMap<>();
                for (Entry<Long, Long> entry : persistentComponents.entries()) {
                    components.put(entry.getKey(), entry.getValue());
                }
                componentsPerContext.put(loopContext.getContext(), components);
            }

            //for a new vertex, initialize adjacency list
            adjacencyList.computeIfAbsent(loopContext.getKey(), k -> new ArrayList<>());

            Long component = components.get(loopContext.getKey());

            if (component == null) {
                component = loopContext.getKey();
                //save component of new vertex
                components.put(loopContext.getKey(), component);
            }

            for (Tuple2<Long, Long> edge : edges) {
                //add new neighbor
                adjacencyList.get(loopContext.getKey()).add(edge.f1);
                //send own component to new neighbor
                out.collect(Either.Left(new Tuple2<>(edge.f1, component)));
            }

        }

        @Override
        public void step(LoopContext<Long> loopContext, Iterable<Tuple2<Long, Long>> componentMessages,
            Collector<Either<Tuple2<Long, Long>, Tuple2<Long, Long>>> out) throws Exception {


            Map<Long, List<Long>> adjacencyList = neighborsPerContext.get(loopContext.getContext());
            if (adjacencyList == null) {
                //load persistent neighbors
                adjacencyList = new HashMap<>();
                for (Entry<Long, List<Long>> entry : persistentGraph.entries()) {
                    adjacencyList.put(entry.getKey(), entry.getValue());
                }
                neighborsPerContext.put(loopContext.getContext(), adjacencyList);
            }

            Map<Long, Long> components = componentsPerContext.get(loopContext.getContext());
            if (components == null) {
                //load persistent components
                adjacencyList = new HashMap<>();
                for (Entry<Long, List<Long>> entry : persistentGraph.entries()) {
                    adjacencyList.put(entry.getKey(), entry.getValue());
                }
                componentsPerContext.put(loopContext.getContext(), components);
            }

            Long oldComponent;

            assert components != null;
            Long newComponent = oldComponent = components.get(loopContext.getKey());


            for (Tuple2<Long, Long> msg : componentMessages) {
                //update component
                if (msg.f1 < newComponent) {
                    newComponent = msg.f1;
                }
            }

            if(!newComponent.equals(oldComponent)) {
                //save new component locally
                components.put(loopContext.getKey(), newComponent);
                //send new component around
                for (long neighbor : adjacencyList.get(loopContext.getKey())) {
                    out.collect(Either.Left(new Tuple2<>(neighbor, newComponent)));
                }
            }


        }

        @Override
        public void onTermination(LoopContext<Long> loopContext,
            Collector<Either<Tuple2<Long, Long>, Tuple2<Long, Long>>> out) throws Exception {

            if(componentsPerContext.containsKey(loopContext.getContext())) {
                persistentGraph.putAll(neighborsPerContext.get(loopContext.getContext()));
                persistentComponents.putAll(componentsPerContext.get(loopContext.getContext()));
                for(Entry<Long, Long> entry: componentsPerContext.get(loopContext.getContext()).entrySet()) {
                    out.collect(Either.Right(new Tuple2<>(entry.getKey(), entry.getValue())));
                }
                System.out.println("STORE :: persistent graph: " + persistentGraph.entries() + "; persistent components: " + persistentComponents.entries());

            }

        }

        private void initState(LoopContext<Long> ctx) {
            if (!listStateDesc.isSerializerInitialized()) {
                listStateDesc.initializeSerializerUnlessSet(ctx.getRuntimeContext().getExecutionConfig());
            }
            if (persistentGraph == null) {
                persistentGraph = ctx.getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("graph", LongSerializer.INSTANCE, listStateDesc.getSerializer()));
                persistentComponents = ctx.getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("components", LongSerializer.INSTANCE, LongSerializer.INSTANCE));

            }
        }
    }
}
