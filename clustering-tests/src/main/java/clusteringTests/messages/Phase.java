package clusteringTests.messages;

public enum Phase {

    /**
     * different messages of one phase can be processed in the same superstep, but never messages of different phases
     */
    ADD_VERTICES, ADD_NEIGHBORS, INITIAL_ANNOUNCEMENTS, COVERAGE_ANNOUNCEMENTS_AND_PREPARATIONS, CENTER_ANNOUNCEMENTS;

}
