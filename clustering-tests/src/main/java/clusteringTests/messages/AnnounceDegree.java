package clusteringTests.messages;

public class AnnounceDegree implements ClusteringMessage {

    @Override
    public Phase getPhase() {
        return Phase.ADD_NEIGHBORS;
    }

}
