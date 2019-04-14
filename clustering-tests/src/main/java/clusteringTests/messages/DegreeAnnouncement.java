package clusteringTests.messages;

public class DegreeAnnouncement implements ClusteringMessage {

    private final long degree;

    public DegreeAnnouncement(long degree) {
        this.degree = degree;
    }

    public long getDegree() {
        return degree;
    }

    @Override
    public Phase getPhase() {
        return Phase.INITIAL_ANNOUNCEMENTS;
    }
}
