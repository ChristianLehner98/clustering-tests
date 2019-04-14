package clusteringTests;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * A source for vertices with IDs increasing by one with random neighbors from all other existing vertices
 */
public class CountingVertexSource extends RichSourceFunction<Tuple2<Long, Set<Long>>> {

    private volatile boolean isRunning = true;
    private Random ran = new Random();
    private long vertices;
    private double edgeProbability;
    private long currentVertex = 0;
    private long currentWatermark = 0;
    private int milliseconds; //time before a new edge is created (not including computing time)

    public CountingVertexSource(long vertices, double edgeProbability, int milliseconds) {
        this.vertices = vertices;
        this.edgeProbability = edgeProbability;
        this.milliseconds = milliseconds;
    }

    @Override
    public void run(SourceContext<Tuple2<Long, Set<Long>>> ctx) throws Exception {
        while (isRunning && currentVertex < vertices) {
            Set<Long> neighbors = new HashSet<>();
            for (long otherVertex = 0; otherVertex < currentVertex; otherVertex++) {
                if (ran.nextDouble() < edgeProbability) {
                    neighbors.add(otherVertex + 1);
                }
            }
            ctx.collectWithTimestamp(new Tuple2<>((currentVertex++) + 1, neighbors), currentVertex*milliseconds);
            if(currentVertex*milliseconds - 1 > currentWatermark + 100 || currentVertex == vertices) {
                currentWatermark = currentVertex*milliseconds - 1;
                ctx.emitWatermark(new Watermark(currentWatermark));
            }
            Thread.sleep(milliseconds);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
