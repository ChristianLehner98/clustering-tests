package clusteringTests.messages;

public class NeighborAnnouncement  implements ClusteringMessage {

    private final boolean center;
    private final long covered;

    public NeighborAnnouncement(boolean center, long covered) {
        this.center = center;
        this.covered = covered;
    }

    public boolean isCenter() {
        return center;
    }

    public long getCovered() {
        return covered;
    }

    @Override
    public Phase getPhase() {
        return Phase.INITIAL_ANNOUNCEMENTS;
    }
}
