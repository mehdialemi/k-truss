package ir.ac.sbu.sbm;

/**
 * Encapsulate edge
 */
public class Edge {
    public int v1;
    public int v2;

    public Edge() {}

    public Edge(int v1, int v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return super.equals(obj);
        Edge edge = (Edge) obj;
        return this.v1 == edge.v1 && this.v2 == edge.v2;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(v1);
    }
}
