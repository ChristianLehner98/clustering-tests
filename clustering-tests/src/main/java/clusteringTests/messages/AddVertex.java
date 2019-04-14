package clusteringTests.messages;

public class AddVertex implements ClusteringMessage {


    @Override
    public Phase getPhase() {
        return Phase.ADD_VERTICES;
    }
}
