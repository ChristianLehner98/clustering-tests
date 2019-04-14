package clusteringTests;


import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * the information saved for a vertex for the clustering algorithm
 * @param <K> the type of the vertex IDs (must be comparable to other elements of the same type)
 */
public class ClusteringVertexValue<K extends Comparable<K>> {

    private K ownID;
    private SimpleSimClusVertexValue ownValue;
    private Tuple2<K, Long> maxNeighborDegree;
    private Tuple2<K, Long> minDegreeNonCenter;
    private long neighborsWithCoverageOne;
    private Map<K, SimpleSimClusVertexValue> neighbors;

    /**
     * initialize the vertex value upon insertion of the vertex
     * @param ownID the ID of the vertex
     * @param neighbors a set of the neighbors of the vertex that are already in the graph
     */
    public ClusteringVertexValue(K ownID, Set<K> neighbors) {
        this.ownID = ownID;
        this.ownValue = new SimpleSimClusVertexValue();
        this.ownValue.degree = neighbors.size();
        this.maxNeighborDegree = null;
        this.minDegreeNonCenter = null;
        this.neighborsWithCoverageOne = 0;
        this.neighbors = new HashMap<>();
        for (K neighbor : neighbors) {
            this.neighbors.put(neighbor, new SimpleSimClusVertexValue());
        }
    }

    /**
     * gets the number of other vertices this vertex is adjacent to
     * @return the degree of the vertex
     */
    public long getDegree() {
        return ownValue.degree;
    }

    /**
     * gets the number of centers this vertex is covered by (adjacent centers + the vertex itself if it is a center)
     * @return the coverage number of the vertex
     */
    public long getCovered() {
        return ownValue.covered;
    }

    /**
     * informs if the vertex is curently a center
     * @return true if it is a center, false otherwise
     */
    public boolean isCenter() {
        return ownValue.center;
    }

    /**
     * change the center status of the vertex
     * @param center true if it should be a center, false if it should be a non-center
     */
    public void setCenter(boolean center) {
        if (ownValue.center != center) {
            ownValue.center = center;
            ownValue.covered = center ? ownValue.covered + 1 : ownValue.covered - 1;
        }
    }

    /**
     * gets the set of all the neighbors of the vertex
     * @return a set containing all vertices the vertex is adjacent to
     */
    public Set<K> getNeighbors() {
        return neighbors.keySet();
    }

    /**
     * adds another adjacent vertex
     * @param neighbor the new neighbor
     */
    public void addNeighbor(K neighbor) {
        neighbors.put(neighbor, new SimpleSimClusVertexValue());
        ownValue.degree++;
    }

    /**
     * removes an adjacent vertex
     * @param neighbor the removed neighbor
     */
    public void removeNeighbor(K neighbor) {
        neighbors.put(neighbor, new SimpleSimClusVertexValue());
        ownValue.degree--;
    }

    /**
     * updates information of a certain neighbor
     * @param neighbor the neighbor whose coverage number should be updated
     * @param covered the new coverage number of the neighbor
     */
    public void setCoveredNeighbor(K neighbor, long covered) {
        SimpleSimClusVertexValue value = neighbors.get(neighbor);
        long oldCovered = value.covered;
        if (covered != oldCovered) {
            value.covered = covered;
            //this is important for centers to determine if they are redundant
            if (oldCovered == 1) {
                neighborsWithCoverageOne--;
            } else if (covered == 1) {
                neighborsWithCoverageOne++;
            }
        }
        neighbors.put(neighbor, value);
    }

    /**
     * updates information of a certain neighbor
     * @param neighbor the neighbor whose degree should be updated
     * @param degree the new degree of the neighbor
     */
    public void setDegreeNeighbor(K neighbor, long degree) {
        SimpleSimClusVertexValue value = neighbors.get(neighbor);
        if(value == null) {
            System.err.println(ownID + ": NullNeighbor" + neighbor);
        }
        long oldDegree = value.degree;
        if (oldDegree != degree) {
            value.degree = degree;
            //this is important for finding correct centers and checking if center is illegal
            if (maxNeighborDegree == null
                || compareDegrees(neighbor, degree, maxNeighborDegree.f0, maxNeighborDegree.f1) > 0) {
                maxNeighborDegree = new Tuple2<>(neighbor, degree);
            } else if (neighbor.equals(maxNeighborDegree.f0)) {
                findNewMaxNeighborDegree();
            }
            if (!value.center) {
                if (minDegreeNonCenter == null || degree < minDegreeNonCenter.f1) {
                    minDegreeNonCenter = new Tuple2<>(neighbor, degree);
                } else if (neighbor.equals(minDegreeNonCenter.f0)) {
                    findNewMinDegreeNonCenter();
                }
            }
        }
        neighbors.put(neighbor, value);
    }

    /**
     * updates information of a certain neighbor
     * @param neighbor the neighbor whose center status should be updated
     * @param center true if the neighbor should be considered a center, false if it should be considered a non-center
     */
    public void setCenterNeighbor(K neighbor, boolean center) {
        SimpleSimClusVertexValue value = neighbors.get(neighbor);
        if (value.center != center) {
            value.center = center;
            //automatically update own coverage value
            ownValue.covered = center ? ownValue.covered + 1 : ownValue.covered - 1;
            //this is important for checking if centers are illegal
            if (center && minDegreeNonCenter != null && neighbor.equals(minDegreeNonCenter.f0)) {
                findNewMinDegreeNonCenter();
            } else if (!center && (minDegreeNonCenter == null || value.degree < minDegreeNonCenter.f1)) {
                minDegreeNonCenter = new Tuple2<>(neighbor, value.degree);
            }
        }
        neighbors.put(neighbor, value);
    }

    /**
     * This method should be used if the minimal degree of any adjacent non-center should be newly determined.
     * from outside this class this should be done once when all information for a new vertex is available, after that
     * is is done automatically by other methods within this class.
     *
     * This information is important because for a legal center this number cannot be higher than its own degree.
     */
    public void findNewMinDegreeNonCenter() {
        minDegreeNonCenter = null;
        for (Entry<K, SimpleSimClusVertexValue> sscvv : neighbors.entrySet()) {
            if (!sscvv.getValue().center && (minDegreeNonCenter == null
                || sscvv.getValue().degree < minDegreeNonCenter.f1)) {
                minDegreeNonCenter = new Tuple2<>(sscvv.getKey(), sscvv.getValue().degree);
            }
        }
    }

    /**
     * This method should be used if the maximal degree of any adjacent vertex should be newly determined.
     * from outside this class this should be done once when all information for a new vertex is available, after that
     * is is done automatically by other methods within this class.
     *
     * This information is important for finding an ideal center in case the vertex is at any stage uncovered
     */
    public void findNewMaxNeighborDegree() {
        maxNeighborDegree = null;
        for (Entry<K, SimpleSimClusVertexValue> sscvv : neighbors.entrySet()) {
            if (maxNeighborDegree == null
                || compareDegrees(sscvv.getKey(), sscvv.getValue().degree, maxNeighborDegree.f0, maxNeighborDegree.f1)
                > 0) {
                maxNeighborDegree = new Tuple2<>(sscvv.getKey(), sscvv.getValue().degree);
            }
        }
    }

    /**
     * checks if the vertex is an illegal center; does NOT check redundancy
     * @return true for an illegal center, false for both non-centers and legal centers
     */
    public boolean checkIllegalCenter() {
        return ownValue.center && ownValue.degree != 0 && (minDegreeNonCenter == null || ownValue.degree < minDegreeNonCenter.f1);
    }

    /**
     * checks if the vertex is a redundant center; does NOT check legality (rule 2 from the paper)
     * @return true for a redundant center, false for both non-centers and necessary centers
     */
    public boolean checkRedundantCenter() {
        return ownValue.center && neighborsWithCoverageOne == 0 && ownValue.covered > 1;
    }

    /**
     * checks if this vertex has strictly higher degree than all of its neighbors. This signalizes that it should be
     * considered as a center
     * @return true if the vertex has strictly higher degree than all of its neighbors, otherwise false
     */
    public boolean checkStrictlyHighestDegree() {
        if(ownValue == null) {
            System.err.println(ownID + ": own value null");
        }
        if(maxNeighborDegree == null) {
            System.err.println(ownID + ": maxND null");
        }
        return ownValue.degree == 0 || ownValue.degree > maxNeighborDegree.f1;
    }

    /**
     * gets the vertex with the highest degree among this vertex itself and all of its neighbors, which would be best
     * fitted as a center if this vertex is currently uncovered
     * @return the vertex with the highest degree among this vertex itself and all of its neighbors. As a tie-breaker
     * the key of the vertices is used
     */
    public K getVertexWithHighestDegree() {
        if (ownValue.degree == 0) {
            return ownID;
        } else {
            return (compareDegrees(ownID, ownValue.degree, maxNeighborDegree.f0, maxNeighborDegree.f1) > 0) ? ownID
                : maxNeighborDegree.f0;
        }
    }

    private int compareDegrees(K key1, long deg1, K key2, long deg2) {
        if (deg1 > deg2) {
            return 1;
        } else if (deg1 < deg2) {
            return -1;
        } else {
            return key1.compareTo(key2);
        }
    }

    /**
     * a very simple class for saving basic information about a vertex
     */
    private static class SimpleSimClusVertexValue {

        long degree = 0;
        long covered = 0;
        boolean center = false;

        @Override
        public String toString() {
            return "SimpleSimClusVertexValue{" +
                "degree=" + degree +
                ", covered=" + covered +
                ", center=" + center +
                '}';
        }
    }

    @Override
    public String toString() {
        return "ClusteringVertexValue{" +
            "ownID=" + ownID +
            ", ownValue=" + ownValue +
            ", maxNeighborDegree=" + maxNeighborDegree +
            ", minDegreeNonCenter=" + minDegreeNonCenter +
            ", neighborsWithCoverageOne=" + neighborsWithCoverageOne +
            ", neighbors=" + neighbors +
            '}';
    }
}
