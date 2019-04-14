package clusteringTests.messages;

public class DeferCenterElection implements ClusteringMessage {

    private final int ttl;


    public DeferCenterElection(int ttl) {
        this.ttl = ttl;
    }

    public int getTtl() {
        return ttl;
    }

    @Override
    public Phase getPhase() {
        if(ttl == 2) {
            return Phase.INITIAL_ANNOUNCEMENTS;
        } else if(ttl == 1) {
            return Phase.COVERAGE_ANNOUNCEMENTS_AND_PREPARATIONS;
        } else {
            return Phase.CENTER_ANNOUNCEMENTS;
        }
    }
}
