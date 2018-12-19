package ir.ac.sbu.sbm;

import it.unimi.dsi.fastutil.bytes.ByteList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Stream;

public class Triangle {

    public static final int META_LEN = 4;
    public static final byte W_SIGN = (byte) 0;
    public static final byte V_SIGN = (byte) 1;
    public static final byte U_SIGN = (byte) 2;

    public static JavaPairRDD <Edge, int[]> createTSet(JavaPairRDD <Integer, int[]> neighbors) {
        JavaPairRDD <Integer, int[]> fonl = fonl(neighbors);
        JavaPairRDD <Integer, Iterable <int[]>> candidates = fonl.filter(t -> t._2.length > 2)
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
                }).groupByKey();

        return fonl.cogroup(candidates)
                .flatMapToPair(t -> {
                    Map<Edge, IntList> wMap = new HashMap <>();
                    Map<Edge, IntList> vMap = new HashMap <>();
                    Map<Edge, IntList> uMap = new HashMap <>();

                    int[] fVal = t._2._1.iterator().next();
                    Arrays.sort(fVal, 1, fVal.length);
                    int v = t._1;

                    for (Iterable <int[]> cValIter : t._2._2) {
                        for (int[] cVal : cValIter) {

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

                                    wMap.computeIfAbsent(uv, k -> new IntArrayList(cVal.length)).add(w);
                                    vMap.computeIfAbsent(uw, k -> new IntArrayList(1)).add(v);
                                    uMap.computeIfAbsent(vw, k -> new IntArrayList(1)).add(u);

                                    fi++;
                                    ci++;
                                }
                            }
                        }
                    }

                    Stream <Tuple2 <Edge, Tuple2 <Byte, int[]>>> wStream = wMap.entrySet()
                            .stream()
                            .map(entry -> new Tuple2 <>(entry.getKey(), new Tuple2 <>(W_SIGN, entry.getValue().toIntArray())));

                    Stream <Tuple2 <Edge, Tuple2 <Byte, int[]>>> vStream = vMap.entrySet()
                            .stream()
                            .map(entry -> new Tuple2 <>(entry.getKey(), new Tuple2 <>(V_SIGN, entry.getValue().toIntArray())));

                    Stream <Tuple2 <Edge, Tuple2 <Byte, int[]>>> vwStream = Stream.concat(wStream, vStream);

                    Stream <Tuple2 <Edge, Tuple2 <Byte, int[]>>> uStream = uMap.entrySet()
                            .stream()
                            .map(entry -> new Tuple2 <>(entry.getKey(), new Tuple2 <>(U_SIGN, entry.getValue().toIntArray())));

                    return Stream.concat(vwStream, uStream).iterator();
                })
                .groupByKey()
                .mapValues(values -> {
                    int wSize = 0;
                    int vSize = 0;
                    int uSize = 0;

                    for (Tuple2 <Byte, int[]> value : values) {
                        switch (value._1) {
                            case W_SIGN: wSize += value._2.length; break;
                            case V_SIGN: vSize += value._2.length; break;
                            case U_SIGN: uSize += value._2.length; break;
                        }
                    }

                    int sup = wSize + vSize + uSize;
                    int[] result = new int[META_LEN + sup];
                    int wOffset = META_LEN;
                    int vOffset = wOffset + wSize;
                    int uOffset = vOffset + vSize;
                    result[0] = sup;
                    result[1] = vOffset;
                    result[2] = uOffset;
                    result[3] = result.length;

                    for (Tuple2 <Byte, int[]> value : values) {
                        switch (value._1) {
                            case W_SIGN:
                                System.arraycopy(value._2, 0, result, wOffset, value._2.length);
                                wOffset += value._2.length;
                                break;
                            case V_SIGN:
                                System.arraycopy(value._2, 0, result, vOffset, value._2.length);
                                vOffset += value._2.length;
                                break;
                            case U_SIGN:
                                System.arraycopy(value._2, 0, result, uOffset, value._2.length);
                                uOffset += value._2.length;
                                break;
                        }
                    }
                    return result;
                }).persist(StorageLevel.MEMORY_AND_DISK());
    }

    private static JavaPairRDD <Integer, int[]> fonl(JavaPairRDD <Integer, int[]> neighbors) {
        return neighbors
//                .repartition(numPartition)
                .flatMapToPair(t -> {
                    int deg = t._2.length;
                    if (deg == 0)
                        return Collections.emptyIterator();

                    int[] vd = new int[]{t._1, deg};
                    List <Tuple2 <Integer, int[]>> degreeList = new ArrayList <>(deg);

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

                    for (int[] vd : v._2) {
                        degree++;
                    }

                    List <int[]> list = new ArrayList <>();
                    for (int[] vd : v._2)
                        if (vd[1] > degree || (vd[1] == degree && vd[0] > v._1))
                            list.add(vd);

                    Collections.sort(list, (a, b) -> {
                        int diff = a[1] - b[1];
                        if (diff == 0)
                            return a[0] - b[0];
                        return diff;
                    });

                    int[] higherDegs = new int[list.size() + 1];
                    higherDegs[0] = degree;
                    for (int i = 1; i < higherDegs.length; i++)
                        higherDegs[i] = list.get(i - 1)[0];

                    return new Tuple2 <>(v._1, higherDegs);
                }).persist(StorageLevel.MEMORY_AND_DISK());
    }
}
