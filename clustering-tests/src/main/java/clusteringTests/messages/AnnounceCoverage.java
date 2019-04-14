package clusteringTests.messages;

public class AnnounceCoverage implements ClusteringMessage {

    @Override
    public Phase getPhase() {
        return Phase.CENTER_ANNOUNCEMENTS;
    }
}
