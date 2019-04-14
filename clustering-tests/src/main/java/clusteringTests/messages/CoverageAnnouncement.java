package clusteringTests.messages;

public class CoverageAnnouncement implements ClusteringMessage {

    private final long coverage;

    public CoverageAnnouncement(long coverage) {
        this.coverage = coverage;
    }


    public long getCoverage() {
        return coverage;
    }

    @Override
    public Phase getPhase() {
        return Phase.COVERAGE_ANNOUNCEMENTS_AND_PREPARATIONS;
    }
}
