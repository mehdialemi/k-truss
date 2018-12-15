package ir.ac.sbu.sbm;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

public class Triangle {

    public static final int META_LEN = 4;
    public static final byte W_SIGN = (byte) 0;
    public static final byte V_SIGN = (byte) 1;
    public static final byte U_SIGN = (byte) 2;

    public static JavaPairRDD <Edge, int[]> createTSet(JavaPairRDD <Integer, int[]> neighbors) {
        JavaPairRDD <Integer, int[]> fonl = fonl(neighbors);
        JavaPairRDD <Integer, int[]> candidates = fonl.filter(t -> t._2.length > 2)
                .flatMapToPair(t -> {

                    int size = t._2.length - 1; // one is for the first index holding node's degree

                    if (size == 1)
                        return Collections.emptyIterator();

                    List <Tuple2 <Integer, int[]>> output;
                    output = new ArrayList <>(size);

                    for (int index = 1; index < size; index++) {
                        int len = size - index;
                        int[] cvalue = new int[len + 1];
                        cvalue[0] = t._1; // First vertex in the triangle
                        System.arraycopy(t._2, index + 1, cvalue, 1, len);
                        Arrays.sort(cvalue, 1, cvalue.length); // quickSort to comfort with fonl
                        output.add(new Tuple2 <>(t._2[index], cvalue));
                    }

                    return output.iterator();
                });

        return fonl.cogroup(candidates)
                .mapPartitionsToPair(partitions -> {
                    Map <Edge, Tuple2 <IntList, ByteList>> map = new HashMap <>();
                    while (partitions.hasNext()) {
                        Tuple2 <Integer, Tuple2 <Iterable <int[]>, Iterable <int[]>>> t = partitions.next();
                        int[] fVal = t._2._1.iterator().next();
                        Arrays.sort(fVal, 1, fVal.length);
                        int v = t._1;
                        for (int[] cVal : t._2._2) {
                            int u = cVal[0];
                            Edge uv = new Edge(u, v);

                            // The intersection determines triangles which u and vertex are two of their vertices.
                            // Always generate and edge (u, vertex) such that u < vertex.
                            int fi = 1;
                            int ci = 1;
                            while (fi < fVal.length && ci < cVal.length) {
                                if (fVal[fi] < cVal[ci])
                                    fi++;
                                else if (fVal[fi] > cVal[ci])
                                    ci++;
                                else {
                                    int w = fVal[fi];
                                    Edge uw = new Edge(u, w);
                                    Edge vw = new Edge(v, w);
                                    Tuple2 <IntList, ByteList> tuple;

                                    tuple = map.computeIfAbsent(uv, k -> new Tuple2 <>(new IntArrayList(), new ByteArrayList()));
                                    tuple._1.add(w);
                                    tuple._2.add(W_SIGN);

                                    tuple = map.computeIfAbsent(uw, k -> new Tuple2 <>(new IntArrayList(), new ByteArrayList()));
                                    tuple._1.add(v);
                                    tuple._2.add(V_SIGN);

                                    tuple = map.computeIfAbsent(vw, k -> new Tuple2 <>(new IntArrayList(), new ByteArrayList()));
                                    tuple._1.add(u);
                                    tuple._2.add(U_SIGN);

                                    fi++;
                                    ci++;
                                }
                            }
                        }
                    }

                    Iterator <Tuple2 <Edge, Tuple2 <int[], byte[]>>> result = map.entrySet()
                            .stream()
                            .map(entry -> new Tuple2 <>(
                                    entry.getKey(),
                                    new Tuple2 <>(entry.getValue()._1.toIntArray(), entry.getValue()._2.toByteArray())))
                            .iterator();
                    return result;
                })
                .groupByKey()
                .mapValues(values -> {
                    IntList wList = new IntArrayList();
                    IntList vList = new IntArrayList();
                    IntList uList = new IntArrayList();

                    for (Tuple2 <int[], byte[]> value : values) {
                        int[] vertices = value._1;
                        byte[] signs = value._2;
                        for (int i = 0; i < vertices.length; i++) {
                            switch (signs[i]) {
                                case W_SIGN:
                                    wList.add(vertices[i]);
                                    break;
                                case V_SIGN:
                                    vList.add(vertices[i]);
                                    break;
                                case U_SIGN:
                                    uList.add(vertices[i]);
                                    break;
                            }
                        }
                    }
                    int offsetW = META_LEN;
                    int offsetV = wList.size() + META_LEN;
                    int offsetU = wList.size() + vList.size() + META_LEN;
                    int len = wList.size() + vList.size() + uList.size();
                    int[] set = new int[META_LEN + len];
                    set[0] = len;  // support of edge
                    set[1] = offsetW + wList.size();  // exclusive max offset of w
                    set[2] = offsetV + vList.size();  // exclusive max offset of vertex
                    set[3] = offsetU + uList.size();  // exclusive max offset of u

                    System.arraycopy(wList.toIntArray(), 0, set, offsetW, wList.size());
                    System.arraycopy(vList.toIntArray(), 0, set, offsetV, vList.size());
                    System.arraycopy(uList.toIntArray(), 0, set, offsetU, uList.size());

                    return set;
                }).persist(StorageLevel.MEMORY_AND_DISK());
    }

    private static JavaPairRDD <Integer, int[]> fonl(JavaPairRDD <Integer, int[]> neighbors) {
        return neighbors.flatMapToPair(t -> {
            int deg = t._2.length;
            if (deg == 0)
                return Collections.emptyIterator();

            VertexDeg vd = new VertexDeg(t._1, deg);
            List <Tuple2 <Integer, VertexDeg>> degreeList = new ArrayList <>(deg);

            // Add degree information of the current vertex to its neighbor
            for (int neighbor : t._2) {
                degreeList.add(new Tuple2 <>(neighbor, vd));
            }

            return degreeList.iterator();
        }).groupByKey()
                .mapToPair(v -> {
                    int degree = 0;
                    // Iterate over higherIds to calculate degree of the current vertex
                    if (v._2 == null)
                        return new Tuple2 <>(v._1, new int[]{0});

                    for (VertexDeg vd : v._2) {
                        degree++;
                    }

                    List <VertexDeg> list = new ArrayList <>();
                    for (VertexDeg vd : v._2)
                        if (vd.degree > degree || (vd.degree == degree && vd.vertex > v._1))
                            list.add(vd);

                    Collections.sort(list, (a, b) -> {
                        int x, y;
                        if (a.degree != b.degree) {
                            x = a.degree;
                            y = b.degree;
                        } else {
                            x = a.vertex;
                            y = b.vertex;
                        }
                        return x - y;
                    });

                    int[] higherDegs = new int[list.size() + 1];
                    higherDegs[0] = degree;
                    for (int i = 1; i < higherDegs.length; i++)
                        higherDegs[i] = list.get(i - 1).vertex;

                    return new Tuple2 <>(v._1, higherDegs);
                }).persist(StorageLevel.MEMORY_AND_DISK());
    }
}
