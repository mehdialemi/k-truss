package ir.ac.sbu.sbm;

import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class MaxTruss {
    public static final int META_LEN = 4;

    public static void main(String[] args) throws URISyntaxException {
        Logger.getLogger("org.apache.spark").removeAllAppenders();
        Logger.getLogger("org.apache.spark").setAdditivity(false);

        ArgumentReader argumentReader = new ArgumentReader(args);
        String input = argumentReader.nextString("/home/mehdi/graph-data/com-youtube.ungraph.txt");
        int k = argumentReader.nextInt(4);
        int cores = argumentReader.nextInt(2);
        int partitions = argumentReader.nextInt(4);
        int pm = argumentReader.nextInt(3);
        int kCoreIteration = argumentReader.nextInt(1000);

        SparkConf sparkConf = new SparkConf();
        if (!input.startsWith("hdfs")) {
            sparkConf.set("spark.driver.bindAddress", "localhost");
            sparkConf.setMaster("local[" + cores + "]");
        }

        sparkConf.setAppName("KTruss-" + k + "-" + new File(input).getName() + "-kc(" + kCoreIteration + ")");
        sparkConf.set("spark.driver.memory", "10g");
        sparkConf.set("spark.driver.maxResultSize", "9g");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(new Class[]{int[].class, byte[].class, VertexDeg.class, Edge.class});
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        if (input.startsWith("hdfs")) {
            String masterHost = new URI(sc.master()).getHost();
            sc.setCheckpointDir("hdfs://" + masterHost + "/shared/checkpoint");
        } else {
            sc.setCheckpointDir("/tmp/checkpoint");
        }

        System.out.println("Running k-truss with argument input: " + input + ", k: " + k +
                ", cores: " + cores + ", partitions: " + partitions + ", pm: " + pm +
                ", kCoreIteration: " + kCoreIteration);

        long t1 = System.currentTimeMillis();
        JavaPairRDD <Edge, Integer> subgraph = find(k, sc, input, kCoreIteration, partitions, pm);
        long t2 = System.currentTimeMillis();
        printKCount(subgraph, 1000);
        System.out.println("duration: " + (t2 - t1) + " ms");
    }

    private static void printKCount(JavaPairRDD <Edge, Integer> truss, int size) {
        Map <Integer, Long> kCount = truss.map(kv -> kv._2).countByValue();
        SortedMap <Integer, Long> sortedMap = new TreeMap <>(kCount);
        int count = 0;
        int sum = 0;
        for (Map.Entry <Integer, Long> entry : sortedMap.entrySet()) {
            sum += entry.getValue();
            if (++count > size) {
                continue;
            }
            System.out.println("K: " + entry.getKey() + ", entry: " + entry.getValue());
        }
        System.out.println("sum: " + sum);
    }

    public static JavaPairRDD <Edge, Integer> find(int k, JavaSparkContext sc, String input,
                                                   int kCoreIterations, int numPartitions, int pm) {
        JavaPairRDD <Integer, Integer> edges = EdgeLoader.load(sc, input);
        JavaPairRDD <Integer, int[]> neighbors = EdgeLoader.createNeighbors(edges)
                .repartition(numPartitions)
                .cache();
        JavaPairRDD <Integer, int[]> kCore = KCore.find(k - 1, neighbors, kCoreIterations);

        JavaPairRDD <Edge, Tuple2 <int[], int[]>> tSetSup = Triangle
                .createTSet(kCore, pm)
                .mapValues(values -> {
                    if (values[0] == 1)
                        values[0] = -1;
                    return new Tuple2 <>(values, new int[]{});
                }).cache();

        long eCount = tSetSup.count();
        System.out.println("eCount: " + eCount);

        int max = 5;
        int min = 2;
        JavaPairRDD <Edge, Integer> maxTruss = sc.parallelizePairs(new ArrayList <>());

        int iteration = 0;
        int multi = 2;
        while (true) {

            iteration++;

            if (iteration % 50 == 0)
                tSetSup.checkpoint();

            long maxSup = max;
            long minSup = min;

            JavaPairRDD <Edge, Iterable <int[]>> updates = tSetSup
                    .filter(kv -> kv._2._1[0] < 0 && Math.abs(kv._2._1[0]) < maxSup)
                    .flatMapToPair(kv -> {
                        Edge e = kv._1;
                        int[] vertices = kv._2._1;
                        int[] supports = kv._2._2;
                        int eSup = Math.abs(vertices[0]);
                        int[] sv2 = new int[]{eSup, e.v2};
                        int[] sv1 = new int[]{eSup, e.v1};
                        int i = META_LEN;

                        List <Tuple2 <Edge, int[]>> out = new ArrayList <>();
                        for (; i < vertices[1]; i++) {
                            int idx = i - META_LEN;
                            if (supports.length > 0 && supports[idx] < eSup)
                                continue;
                            out.add(new Tuple2 <>(new Edge(e.v1, vertices[i]), sv2));
                            out.add(new Tuple2 <>(new Edge(e.v2, vertices[i]), sv1));
                        }

                        for (; i < vertices[2]; i++) {
                            int idx = i - META_LEN;
                            if (supports.length > 0 && supports[idx] < eSup)
                                continue;
                            out.add(new Tuple2 <>(new Edge(e.v1, vertices[i]), sv2));
                            out.add(new Tuple2 <>(new Edge(vertices[i], e.v2), sv1));
                        }

                        for (; i < vertices[3]; i++) {
                            int idx = i - META_LEN;
                            if (supports.length > 0 && supports[idx] < eSup)
                                continue;
                            out.add(new Tuple2 <>(new Edge(vertices[i], e.v1), sv2));
                            out.add(new Tuple2 <>(new Edge(vertices[i], e.v2), sv1));
                        }

                        return out.iterator();
                    }).groupByKey();

            tSetSup = tSetSup
                    .leftOuterJoin(updates)
                    .mapValues(values -> {
                        Tuple2 <int[], int[]> tVal = values._1;
                        Optional <Iterable <int[]>> right = values._2;
                        int[] vertices = tVal._1;
                        int eSup = Math.abs(vertices[0]);

                        if (eSup < maxSup)
                            vertices[0] = eSup;

                        if (!right.isPresent() || eSup < minSup)
                            return tVal;

                        Int2IntMap updateMap = null;
                        for (int[] sv : values._2.get()) {
                            int support = sv[0];
                            if (support >= eSup) {
                                continue;
                            }

                            if (updateMap == null)
                                updateMap = new Int2IntOpenHashMap();

                            int vertex = sv[1];
                            int sup = updateMap.getOrDefault(vertex, Integer.MAX_VALUE);
                            if (support < sup) {
                                updateMap.put(vertex, support);
                            }
                        }

                        if (updateMap == null || updateMap.size() == 0) {
                            tVal._1[0] = eSup;
                            return tVal;
                        }

                        int[] supports = tVal._2;
                        if (supports.length == 0) {
                            supports = new int[vertices.length - META_LEN];
                            for (int i = 0; i < supports.length; i++) {
                                supports[i] = eSup;
                            }
                        }

                        Int2IntAVLTreeMap map = new Int2IntAVLTreeMap(Comparator.comparingInt(a -> -a));
                        for (int i = META_LEN; i < vertices.length; i++) {
                            int idx = i - META_LEN;

                            if (updateMap.size() == 0) {
                                break;
                            }

                            int vertex = vertices[i];
                            if (!updateMap.containsKey(vertex))
                                continue;

                            int sup = updateMap.remove(vertex);
                            if (sup < supports[idx]) {
                                supports[idx] = sup;
                            }
                        }

                        int eSupCount = 0;
                        for (int support : supports) {
                            if (support == eSup) {
                                eSupCount ++;
                                continue;
                            }
                            map.addTo(support, 1);
                        }

                        if (eSupCount == eSup) {
                            vertices[0] = eSup;
                            return new Tuple2 <>(vertices, supports);
                        }

                        map.put(eSupCount, eSupCount);
                        int sum = 0;
                        int sup = 0;
                        for (Int2IntMap.Entry entry : map.int2IntEntrySet()) {
                            sup = entry.getIntKey();
                            if (sup < sum) {
                                sup = sum;
                                break;
                            }

                            sum = sum + entry.getIntValue();
                            if (sup <= sum)
                                break;
                        }

                        for (int i = 0; i < supports.length; i++) {
                            if (supports[i] > sup)
                                supports[i] = sup;
                        }
                        vertices[0] = -sup;

                        return new Tuple2 <>(vertices, supports);
                    }).persist(StorageLevel.MEMORY_AND_DISK());

            JavaPairRDD <Edge, Integer> truss = tSetSup
                    .filter(kv -> kv._2._1[0] > 0 && kv._2._1[0] < minSup)
                    .mapValues(v -> v._1[0])
                    .persist(StorageLevel.MEMORY_AND_DISK());

            long tCount = truss.count();
            System.out.println("truss count: " + tCount + ", minSup: " + minSup + ", maxSup: " + maxSup);
            maxTruss = maxTruss.union(truss);

            if (tCount == 0) {
                min++;
                max *= multi;
                multi *= 2;
            } else {
                multi = 2;
                max++;
                tSetSup = tSetSup.subtractByKey(truss).persist(StorageLevel.MEMORY_AND_DISK());
            }

            if (tCount == 0 && max > eCount)
                break;
        }

        return maxTruss;
    }
}
