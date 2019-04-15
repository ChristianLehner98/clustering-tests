package clusteringTests;

import clusteringTests.messages.AddNeighbor;
import clusteringTests.messages.AnnounceCoverage;
import clusteringTests.messages.AnnounceDegree;
import clusteringTests.messages.CenterAnnouncement;
import clusteringTests.messages.ClusteringMessage;
import clusteringTests.messages.CoverageAnnouncement;
import clusteringTests.messages.DeferCenterElection;
import clusteringTests.messages.DegreeAnnouncement;
import clusteringTests.messages.MakeCenter;
import clusteringTests.messages.NeighborAnnouncement;
import clusteringTests.messages.RemoveCenter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import org.apache.flink.streaming.api.functions.windowing.LoopContext;
import org.apache.flink.streaming.api.functions.windowing.WindowLoopFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.tasks.progress.FixpointIterationTermination;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

public class Test1 {


    public static void main(String args[]) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //vertices come from a CountingVertexSource with predefined parameters (can be changed manually)
        DataStream<Tuple2<Long, Set<Long>>> input = env.addSource(new CountingVertexSource(151, 0.1, 1000));

        KeyedStream<Tuple2<Long, Set<Long>>, Long> keyedInput = input.keyBy(new KeySelector<Tuple2<Long, Set<Long>>, Long>() {
                @Override
                public Long getKey(Tuple2<Long, Set<Long>> value) throws Exception {
                    return value.f0;
                }
            });

        WindowedStream<Tuple2<Long, Set<Long>>, Long, TimeWindow> windowedInput = keyedInput
            .timeWindow(Time.milliseconds(10000));

        DataStream<String> results = windowedInput.iterateSync(
            new ClusteringWindowLoopFunction(), new FixpointIterationTermination(),
            new FeedbackBuilder<Tuple3<Long, Long, ClusteringMessage>, Long>() {
                @Override
                public KeyedStream<Tuple3<Long, Long, ClusteringMessage>, Long> feedback(
                    DataStream<Tuple3<Long, Long, ClusteringMessage>> input) {
                    return input.keyBy(new KeySelector<Tuple3<Long, Long, ClusteringMessage>, Long>() {
                        @Override
                        public Long getKey(Tuple3<Long, Long, ClusteringMessage> value) throws Exception {
                            return value.f1;
                        }
                    });
                }
            }, new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.of(
                new TypeHint<ClusteringMessage>() {
                })));

        results.print();

        env.execute();
    }

    private static class ClusteringWindowLoopFunction implements
        WindowLoopFunction<Tuple2<Long, Set<Long>>, Tuple3<Long, Long, ClusteringMessage>, String, Tuple3<Long, Long, ClusteringMessage>, Long, TimeWindow>  {

        private Map<List<Long>, Map<Long, ClusteringVertexValueLong>> vertexValuesPerContext = new HashMap<>();
//        private Set<List<Long>> vertexValuesUpdated = new HashSet<>();


        private MapState<Long, ClusteringVertexValueLong> persistentVertexValue = null;

		private Map<Long, ClusteringVertexValueLong> vertexValuesTest = new HashMap<>();

/*		private final ValueStateDescriptor<ClusteringVertexValueLong> vertexStateDescriptor = new ValueStateDescriptor<>("VV", TypeInformation
			.of(new TypeHint<ClusteringVertexValueLong>() {
			}));
*/



        @Override
        public void entry(LoopContext<Long> ctx, Iterable<Tuple2<Long, Set<Long>>> input,
            Collector<Either<Tuple3<Long, Long, ClusteringMessage>, String>> out) throws Exception {

/*            checkAndInitState(ctx);

//			System.out.println(ctx.getContext() + ": ENTRY: " + ctx.getKey() + "; SS: " + ctx.getSuperstep() + " :: " + ctx.getRuntimeContext());

            Map<Long, ClusteringVertexValueLong> vertexValues = vertexValuesPerContext.get(ctx.getContext());
            if (vertexValues == null) {
                vertexValues = new HashMap<>();
                System.out.println("LOAD: " + persistentVertexValue.entries());
                for (Entry<Long, ClusteringVertexValueLong> e : persistentVertexValue.entries()) {
                    vertexValues.put(e.getKey(), e.getValue());
                }
                vertexValuesPerContext.put(ctx.getContext(), vertexValues);
            }
*/
            Tuple2<Long, Set<Long>> next = input.iterator().next();

            if (!next.f0.equals(ctx.getKey())) {
                throw new AssertionError();
            }

            ClusteringVertexValueLong vertexValue = new ClusteringVertexValueLong(next.f0, next.f1);

//            vertexValues.put(ctx.getKey(), vertexValue);

            vertexValuesTest.put(ctx.getKey(), vertexValue);

//            ctx.getRuntimeContext().getState(vertexStateDescriptor).update(vertexValue);

            for (long neighbor : next.f1) {
                out.collect(new Either.Left<>(
                    new Tuple3<>(ctx.getKey(), neighbor, new AddNeighbor())));
            }
            out.collect(new Either.Left<>(new Tuple3<>(ctx.getKey(), ctx.getKey(), new AnnounceDegree())));
            out.collect(new Either.Right<>("added vertex " + ctx.getKey() + ". Neighbors: " + next.f1));


        }

        @Override
        public void step(LoopContext<Long> ctx, Iterable<Tuple3<Long, Long, ClusteringMessage>> input,
            Collector<Either<Tuple3<Long, Long, ClusteringMessage>, String>> out) throws Exception {

            //TODO: investigate why some AddNeighbor messages get lost

//            Map<Long, ClusteringVertexValueLong> vertexValues = vertexValuesPerContext.get(ctx.getContext());

            Iterator<Tuple3<Long, Long, ClusteringMessage>> iterator = input.iterator();
            Tuple3<Long, Long, ClusteringMessage> next = iterator.next();

            if (!next.f1.equals(ctx.getKey())) {
                throw new AssertionError();
            }


/*			ClusteringVertexValueLong vertexValue = ctx.getRuntimeContext().getState(vertexStateDescriptor).value();

			if(vertexValue == null) {
//				System.err.println("state null: " + ctx.getKey() + " :: " + next.f2.getPhase());
				vertexValue = vertexValues.get(ctx.getKey());
			} else {
//				System.err.println("state exists: " + ctx.getKey() + " :: " + next.f2.getPhase());
				vertexValues.put(ctx.getKey(), vertexValue);
			}

			if(vertexValue == null) {
//				vertexValue = vertexValuesTest.get(ctx.getKey());
				vertexValues.put(ctx.getKey(), vertexValue);
			} else {
//				vertexValuesTest.put(ctx.getKey(), vertexValue);
			}

			if(vertexValue != vertexValuesTest.get(ctx.getKey())) {
				System.err.println("state wrong");
			}
*/

//            ClusteringVertexValueLong vertexValue =

            ClusteringVertexValueLong vertexValue = vertexValuesTest.get(ctx.getKey());

            if (vertexValue == null) {
                System.err.println((ctx.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> " + ctx.getKey());
                throw new AssertionError();
            }

//			System.out.println(ctx.getContext() + ": STEP: " + ctx.getKey() + "; SS: " + ctx.getSuperstep() + "; Phase: " + next.f2.getPhase() + " :: " + ctx.getRuntimeContext());

            boolean newVertex;

            switch (next.f2.getPhase()) {
                case ADD_NEIGHBORS:
                    newVertex = false;
                    //iterate over all messages
                    while (true) {
//                        System.out.println(ctx.getContext() + ": STEP: " + ctx.getKey() + "; SS: " + ctx.getSuperstep() + " source: " + next.f0 + "; message: " + next.f2 + " :: " + ctx.getRuntimeContext());
                        if (next.f2 instanceof AddNeighbor) {
                            //the way the insert messages currently work, this if condition should always be true, but it
                            //is there because this might be different in actual usage of the algorithm
                            if (!vertexValue.getNeighbors().contains(next.f0)) {
                                //add the neighbor
                                vertexValue.addNeighbor(next.f0);
                                //send a message back to the new vertex informing them of own center status coverage number
                                out.collect(new Either.Left<>(
                                    new Tuple3<>(ctx.getKey(), next.f0, new NeighborAnnouncement(vertexValue.isCenter(),
                                        vertexValue.getCovered()))));
                            }
                        } else if (next.f2 instanceof AnnounceDegree) {
                            //AnnounceDegree message is a marker that this is a vertex added only in the last superstep
                            newVertex = true;
                        }
                        if (iterator.hasNext()) {
                            next = iterator.next();
                        } else {
                            break;
                        }
                    }
                    if (newVertex) {
                        if (vertexValue.getDegree() == 0) {
                            //isolated vertices are centers automatically but don't have any neghbors they have to inform
                            //about that
                            vertexValue.setCenter(true);
                            out.collect(new Either.Right<>("new center: " + ctx.getKey()));
                        } else {
                            //otherwise the new vertex should be reminded that it eventually has to choose a new center if
                            //it is still uncovered after some time
                            out.collect(new Either.Left<>(
                                new Tuple3<>(ctx.getKey(), ctx.getKey(), new DeferCenterElection(2))));
                        }
                    }
                    //inform all neighbors about new degree
                    for (long neighbor : vertexValue.getNeighbors()) {
                        out.collect(new Either.Left<>(
                            new Tuple3<>(ctx.getKey(), neighbor, new DegreeAnnouncement(vertexValue.getDegree()))));
                    }
                    break;
                case INITIAL_ANNOUNCEMENTS:
                    newVertex = false;
                    //iterate over all messages
                    while (true) {
//                        System.out.println(ctx.getContext() + ": STEP: " + ctx.getKey() + "; SS: " + ctx.getSuperstep() + " source: " + next.f0 + "; message: " + next.f2 + " :: " + ctx.getRuntimeContext());
                        if (next.f2 instanceof NeighborAnnouncement) {
                            boolean neighborCenter = ((NeighborAnnouncement) next.f2).isCenter();
                            long neighborCovered = ((NeighborAnnouncement) next.f2).getCovered();
                            //update center status and coverage number of the neighbor
                            vertexValue.setCoveredNeighbor(next.f0, neighborCovered);
                            vertexValue.setCenterNeighbor(next.f0, neighborCenter);
                        } else if (next.f2 instanceof DegreeAnnouncement) {
                            long neighborDegree = ((DegreeAnnouncement) next.f2).getDegree();
                            //update degree of the neighbor
                            vertexValue.setDegreeNeighbor(next.f0, neighborDegree);
                        } else if (next.f2 instanceof DeferCenterElection) {
                            //the new vertex should be reminded that it eventually has to choose a new center if it is still
                            //uncovered after some time
                            out.collect(new Either.Left<>(new Tuple3<>(ctx.getKey(), ctx.getKey(),
                                new DeferCenterElection(((DeferCenterElection) next.f2).getTtl() - 1))));
                            newVertex = true;
                        }
                        if (iterator.hasNext()) {
                            next = iterator.next();
                        } else {
                            break;
                        }
                    }
                    if (newVertex) {
                        //initialize some last information of the new vertex and send newly gathered coverage number to
                        //all neighbors
                        vertexValue.findNewMaxNeighborDegree();
                        vertexValue.findNewMinDegreeNonCenter();
                        for (long neighbor : vertexValue.getNeighbors()) {
                            out.collect(new Either.Left<>(new Tuple3<>(ctx.getKey(), neighbor,
                                new CoverageAnnouncement(vertexValue.getCovered()))));
                        }
                    }
                    //if center, check if new neighbor degrees or added neighbors have made the center illegal
                    //if yes inform self to remove center status in next step
                    if (vertexValue.checkIllegalCenter()) {
                        out.collect(new Either.Left<>(new Tuple3<>(ctx.getKey(), ctx.getKey(), new RemoveCenter())));
                    }
                    //optional
                    //if a non-center has strictly higher degree than all its neighbors it might be better suited as a
                    //center. This is not necessary according to the rules but mentioned as an optimazition for the SimClus
                    //algorithm
                    if (!vertexValue.isCenter() && vertexValue.checkStrictlyHighestDegree()) {
                        out.collect(new Either.Left<>(new Tuple3<>(ctx.getKey(), ctx.getKey(), new MakeCenter())));
                    }
                    //end_optional
                    break;
                case COVERAGE_ANNOUNCEMENTS_AND_PREPARATIONS:
                    boolean makeCenter = false;
                    boolean removeCenter = false;
                    //iterate over all messages
                    while (true) {
//                        System.out.println(ctx.getContext() + ": STEP: " + ctx.getKey() + "; SS: " + ctx.getSuperstep() + " source: " + next.f0 + "; message: " + next.f2 + " :: " + ctx.getRuntimeContext());
                        if (next.f2 instanceof CoverageAnnouncement) {
                            long neighborCoverage = ((CoverageAnnouncement) next.f2).getCoverage();
                            //update coverage number of the neighbor
                            vertexValue.setCoveredNeighbor(next.f0, neighborCoverage);
                        } else if (next.f2 instanceof DeferCenterElection) {
                            //the new vertex should be reminded that it eventually has to choose a new center if it is still
                            //uncovered after some time
                            out.collect(new Either.Left<>(new Tuple3<>(ctx.getKey(), ctx.getKey(),
                                new DeferCenterElection(((DeferCenterElection) next.f2).getTtl() - 1))));
                        } else if (next.f2 instanceof MakeCenter) {
                            //remember to make this vertex new center in the end
                            makeCenter = true;
                        } else if (next.f2 instanceof RemoveCenter) {
                            //remember to remove this vertex as a center in the end
                            removeCenter = true;
                        }
                        if (iterator.hasNext()) {
                            next = iterator.next();
                        } else {
                            break;
                        }
                    }
                    //if a center realizes it has become redundant it must be removed
                    if (vertexValue.checkRedundantCenter()) {
                        removeCenter = true;
                    }
                    if (makeCenter) {
                        //make this vertex a new center and inform all neighbors about that
                        vertexValue.setCenter(true);
                        for (long neighbor : vertexValue.getNeighbors()) {
                            out.collect(
                                new Either.Left<>(new Tuple3<>(ctx.getKey(), neighbor, new CenterAnnouncement(true))));
                        }
                        //remind to announce new coverage number in the next step
                        out.collect(
                            new Either.Left<>(new Tuple3<>(ctx.getKey(), ctx.getKey(), new AnnounceCoverage())));
                        out.collect(new Either.Right<>("new center: " + ctx.getKey()));
                    } else if (removeCenter) {
                        //make this vertex as a center and inform all neighbors about that
                        vertexValue.setCenter(false);
                        for (long neighbor : vertexValue.getNeighbors()) {
                            out.collect(
                                new Either.Left<>(new Tuple3<>(ctx.getKey(), neighbor, new CenterAnnouncement(false))));
                        }
                        //remember to announce new coverage number in the next step
                        out.collect(
                            new Either.Left<>(new Tuple3<>(ctx.getKey(), ctx.getKey(), new AnnounceCoverage())));
                        out.collect(new Either.Right<>("removed center: " + ctx.getKey()));
                    }
                    break;
                case CENTER_ANNOUNCEMENTS:
                    boolean announceCoverage = false;
                    //iterate over all messages
                    while (true) {
//                        System.out.println(ctx.getContext() + ": STEP: " + ctx.getKey() + "; SS: " + ctx.getSuperstep() + " source: " + next.f0 + "; message: " + next.f2 + " :: " + ctx.getRuntimeContext());
                        if (next.f2 instanceof CenterAnnouncement) {
                            boolean neighborCenter = ((CenterAnnouncement) next.f2).isCenter();
                            //update center status information of the neighbor (automatically updates own coverage number)
                            vertexValue.setCenterNeighbor(next.f0, neighborCenter);
                            //remember to announce new coverage number later
                            announceCoverage = true;
                        } else if (next.f2 instanceof AnnounceCoverage) {
                            //remember to announce new coverage number later
                            announceCoverage = true;
                        }
                        if (iterator.hasNext()) {
                            next = iterator.next();
                        } else {
                            break;
                        }
                    }
                    //if center, check if new neighboring centers have made the center illegal
                    //if yes inform self to remove center status in next step
                    if (vertexValue.checkIllegalCenter()) {
                        out.collect(new Either.Left<>(new Tuple3<>(ctx.getKey(), ctx.getKey(), new RemoveCenter())));
                    }
                    //if a vertex is (still or newly) uncovered tell the vertex with the highest degree among itself
                    //and its neighbors to make itself a center in the next step
                    if (vertexValue.getCovered() == 0) {
                        out.collect(new Either.Left<>(
                            new Tuple3<>(ctx.getKey(), vertexValue.getVertexWithHighestDegree(), new MakeCenter())));
                    }
                    //if the coverage number might have changed, inform all neighbors about the new coverage number
                    if (announceCoverage) {
                        for (long neighbor : vertexValue.getNeighbors()) {
                            out.collect(new Either.Left<>(new Tuple3<>(ctx.getKey(), neighbor,
                                new CoverageAnnouncement(vertexValue.getCovered()))));
                        }
                    }
                    break;
            }

            vertexValuesTest.put(ctx.getKey(), vertexValue);

        }

        @Override
        public void onTermination(LoopContext<Long> ctx,
            Collector<Either<Tuple3<Long, Long, ClusteringMessage>, String>> out) throws Exception {

/*            if (vertexValuesPerContext.containsKey(ctx.getContext())) {

                persistentVertexValue.putAll(vertexValuesPerContext.get(ctx.getContext()));

                System.out.println("FINALIZE: " + ctx.getContext() + " :: " + persistentVertexValue.entries());

            }*/

//            System.out.println("ON_TERMINATION: " + ctx.getContext() + " :: " + vertexValuesTest.entrySet());

        }

        private void checkAndInitState(LoopContext<Long> ctx) throws Exception {
            if (persistentVertexValue == null) {
                persistentVertexValue = ctx.getRuntimeContext()
                    .getMapState(new MapStateDescriptor<>("vertex values", BasicTypeInfo.LONG_TYPE_INFO, TypeInformation
                        .of(new TypeHint<ClusteringVertexValueLong>() {
                        })));
            }
        }
    }

    private static class ClusteringVertexValueLong extends ClusteringVertexValue<Long> implements Serializable {

        /**
         * initialize the vertex value upon insertion of the vertex
         *
         * @param ownID the ID of the vertex
         * @param neighbors a set of the neighbors of the vertex that are already in the graph
         */
        public ClusteringVertexValueLong(Long ownID, Set<Long> neighbors) {
            super(ownID, neighbors);
        }
    }

}
