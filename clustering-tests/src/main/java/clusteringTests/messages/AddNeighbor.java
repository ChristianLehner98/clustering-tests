package clusteringTests.messages;

public class AddNeighbor implements ClusteringMessage {


    @Override
    public Phase getPhase() {
        return Phase.ADD_NEIGHBORS;
    }
}
