package clusteringTests.messages;

public class CenterAnnouncement implements ClusteringMessage {

    private final boolean center;

    public CenterAnnouncement(boolean center) {
        this.center = center;
    }

    public boolean isCenter() {
        return center;
    }

    @Override
    public Phase getPhase() {
        return Phase.CENTER_ANNOUNCEMENTS;
    }

}
