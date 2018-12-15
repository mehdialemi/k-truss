package ir.ac.sbu.sbm;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static ir.ac.sbu.sbm.Triangle.META_LEN;

public class KTruss {
    private static final int INVALID = -1;
    public static final int CHECKPOINT_ITERATION = 50;
    public static final int[] RE_COMP_VAL = new int[] {0, 0};

    public static void main(String[] args) {
        ArgumentReader argumentReader = new ArgumentReader(args);
        String input = argumentReader.nextString("/home/mehdi/graph-data/com-youtube.ungraph.txt");
        int k = argumentReader.nextInt(4);
        int kPlus = argumentReader.nextInt(3);
        int cores = argumentReader.nextInt(2);
        int partitions = argumentReader.nextInt(4);
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

        System.out.println("Running k-truss with argument input: " + input + ", k: " + k +
                ", cores: " + cores + ", partitions: " + partitions +
                ", kCoreIteration: " + kCoreIteration);

        long t1 = System.currentTimeMillis();
        JavaPairRDD <Edge, int[]> subgraph = find(k, k + kPlus, sc, input, partitions, kCoreIteration);
        long t2 = System.currentTimeMillis();
        System.out.println("KTruss edge count: " + subgraph.count() + ", duration: " + (t2 - t1) + " ms");
    }

    public static JavaPairRDD <Edge, int[]> find(int k, int h, JavaSparkContext sc, String input, int partitions,
                                                 int kCoreIterations) {
        JavaPairRDD <Integer, Integer> edges = EdgeLoader.load(sc, input);

        JavaPairRDD <Integer, int[]> neighbors = EdgeLoader.createNeighbors(edges);

        JavaPairRDD <Integer, int[]> kCore = KCore.find(k - 1, neighbors, kCoreIterations);

        long t1 = System.currentTimeMillis();
        JavaPairRDD <Edge, int[]> tSet = Triangle.createTSet(kCore, partitions, h);
        System.out.println("tSet count: " + tSet.count() + ", time: " + (System.currentTimeMillis() - t1) + " ms");

        return process(k, tSet, h);
    }

    private static JavaPairRDD<Edge, int[]> reTSet(JavaPairRDD<Edge, int[]> tSet, int k, int partitions, int h) {
        JavaPairRDD <Integer, Integer> edges = tSet.flatMapToPair(kv -> {
            List<Tuple2<Integer, Integer>> list = new ArrayList<>();
            list.add(new Tuple2 <>(kv._1.v1, kv._1.v2));
            list.add(new Tuple2 <>(kv._1.v2, kv._1.v1));
            return list.iterator();
        });
        JavaPairRDD <Integer, int[]> neighbors = EdgeLoader.createNeighbors(edges);
        JavaPairRDD <Integer, int[]> kCore = KCore.find(k - 1, neighbors, 1);
        long t1 = System.currentTimeMillis();
        JavaPairRDD <Edge, int[]> tSet2 = Triangle.createTSet(kCore, partitions, h);
        System.out.println("tSet count: " + tSet2.count() + ", time: " + (System.currentTimeMillis() - t1) + " ms");
        return tSet2;
    }

    private static JavaPairRDD <Edge, int[]> process(final int k, JavaPairRDD <Edge, int[]> tSet, int h) {
        int numPartitions = tSet.getNumPartitions();

        final int minSup = k - 2;
        Queue <JavaPairRDD <Edge, int[]>> tSetQueue = new LinkedList <>();
        Queue <JavaPairRDD <Edge, int[]>> invQueue = new LinkedList <>();
        tSetQueue.add(tSet);
        long kTrussDuration = 0;
        int invalidsCount = 0;
        int iter = 0;
        while (true) {
            iter++;
            long t1 = System.currentTimeMillis();

            if (iter % CHECKPOINT_ITERATION == 0) {
                tSet.checkpoint();
            }

            // Detect invalid edges by comparing the support of triangle vertex set
            JavaPairRDD <Edge, int[]> invalids = tSet.filter(kv ->
                    (kv._2[0] >= 0 && kv._2[0] < minSup) || (kv._2.length == 2))
                    .cache();
            long rCount = invalids.filter(kv -> kv._2.length == 2).count();
            if(rCount > 0) {
                System.out.println("recompute size: " + rCount);
                tSet = reTSet(tSet, k, numPartitions, h);
                invalids = tSet.filter(kv -> kv._2[0] >= 0 && kv._2[0] < minSup).cache();
            } else {
                invalids = invalids.filter(kv -> kv._2.length != 2).cache();
            }

            long invalidCount = invalids.count();

            // If no invalid edge is found then the program terminates
            if (invalidCount == 0) {
                break;
            }

            invalidsCount += invalidCount;

            if (tSetQueue.size() > 2)
                tSetQueue.remove().unpersist();
            if (invQueue.size() > 2)
                invQueue.remove().unpersist();

            invQueue.add(invalids);

            long t2 = System.currentTimeMillis();
            String msg = "iteration: " + iter + ", invalid edge count: " + invalidCount;
            long iterDuration = t2 - t1;
            kTrussDuration += iterDuration;
            System.out.println(msg + ", duration: " + iterDuration + " ms");

            // The edges in the key part of invalids key-values should be removed. So, we detect other
            // edges of their involved triangle from their triangle vertex set. Here, we determine the
            // vertices which should be removed from the triangle vertex set related to the other edges.
            JavaPairRDD <Edge, Iterable <Integer>> invUpdates = invalids
                    .flatMapToPair(kv -> {
                        int i = META_LEN;

                        Edge e = kv._1;
                        List <Tuple2 <Edge, Integer>> out = new ArrayList <>();
                        for (; i < kv._2[1]; i++) {
                            if (kv._2[i] == INVALID)
                                continue;
                            out.add(new Tuple2 <>(new Edge(e.v1, kv._2[i]), e.v2));
                            out.add(new Tuple2 <>(new Edge(e.v2, kv._2[i]), e.v1));
                        }

                        for (; i < kv._2[2]; i++) {
                            if (kv._2[i] == INVALID)
                                continue;
                            out.add(new Tuple2 <>(new Edge(e.v1, kv._2[i]), e.v2));
                            out.add(new Tuple2 <>(new Edge(kv._2[i], e.v2), e.v1));
                        }

                        for (; i < kv._2[3]; i++) {
                            if (kv._2[i] == INVALID)
                                continue;
                            out.add(new Tuple2 <>(new Edge(kv._2[i], e.v1), e.v2));
                            out.add(new Tuple2 <>(new Edge(kv._2[i], e.v2), e.v1));
                        }

                        return out.iterator();
                    }).groupByKey(numPartitions);

            // Remove the invalid vertices from the triangle vertex set of each remaining (valid) edge.
            tSet = tSet.filter(kv -> kv._2[0] < 0 || kv._2[0] >= minSup).leftOuterJoin(invUpdates)
                    .mapValues(values -> {
                        Optional <Iterable <Integer>> invalidUpdate = values._2;
                        int[] set = values._1;

                        // If no invalid vertex is present for the current edge then return the set value.
                        if (!invalidUpdate.isPresent()) {
                            return set;
                        }

                        IntSet iSet = new IntOpenHashSet();
                        for (int v : invalidUpdate.get()) {
                            iSet.add(v);
                        }

                        if (set[0] < 0) {
                            int nSup = Math.max(0, Math.abs(set[0]) - iSet.size());
                            if (nSup < minSup)
                                return RE_COMP_VAL;

                            set[0] = -nSup;
                            return set;
                        }

                        for (int i = META_LEN; i < set.length; i++) {
                            if (set[i] == INVALID)
                                continue;
                            if (iSet.contains(set[i])) {
                                set[0]--;
                                set[i] = INVALID;
                            }
                        }

                        // When the triangle vertex iSet has no other element then the current edge should also
                        // be eliminated from the current tvSets.
                        return set;
                    })
                    .persist(StorageLevel.MEMORY_AND_DISK());

            tSetQueue.add(tSet);
        }

        System.out.println("kTruss duration: " + kTrussDuration + " ms, invalids: " + invalidsCount);
        return tSet;
    }
}
