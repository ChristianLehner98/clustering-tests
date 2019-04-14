package clusteringTests.messages;

public class RemoveCenter implements ClusteringMessage {


    @Override
    public Phase getPhase() {
        return Phase.COVERAGE_ANNOUNCEMENTS_AND_PREPARATIONS;
    }
}
