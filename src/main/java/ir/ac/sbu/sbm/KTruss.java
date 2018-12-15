package ir.ac.sbu.sbm;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class KTruss {
    private static final int INVALID = -1;
    public static final int META_LEN = 4;
    public static final int CHECKPOINT_ITERATION = 50;

    public static void main(String[] args) {
        ArgumentReader argumentReader = new ArgumentReader(args);
        String input = argumentReader.nextString("/home/mehdi/graph-data/com-youtube.ungraph.txt");
        int k = argumentReader.nextInt(4);
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
        JavaPairRDD <Edge, int[]> subgraph = find(k, sc, input, partitions, kCoreIteration);
        long t2 = System.currentTimeMillis();
        System.out.println("KTruss edge count: " + subgraph.count() + ", duration: " + (t2 - t1) + " ms");
    }

    public static JavaPairRDD <Edge, int[]> find(int k, JavaSparkContext sc, String input, int partitions,
                                                 int kCoreIterations) {
        JavaPairRDD <Integer, Integer> edges = EdgeLoader.load(sc, input);

        JavaPairRDD <Integer, int[]> neighbors = EdgeLoader.createNeighbors(edges);

        JavaPairRDD <Integer, int[]> kCore = KCore.find(k - 1, neighbors, kCoreIterations);

        long t1 = System.currentTimeMillis();
        JavaPairRDD <Edge, int[]> tSet = Triangle.createTSet(kCore, partitions);
        System.out.println("tSet count: " + tSet.count() + ", time: " + (System.currentTimeMillis() - t1) + " ms");

        return process(k - 2, tSet);
    }

    private static JavaPairRDD <Edge, int[]> process(final int minSup, JavaPairRDD <Edge, int[]> tSet) {
        int numPartitions = tSet.getNumPartitions();

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
            JavaPairRDD <Edge, int[]> invalids = tSet.filter(kv -> kv._2[0] < minSup).cache();
            long invalidCount = invalids.count();

            // If no invalid edge is found then the program terminates
            if (invalidCount == 0) {
                break;
            }

            invalidsCount += invalidCount;

            if (tSetQueue.size() > 1)
                tSetQueue.remove().unpersist();
            if (invQueue.size() > 1)
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
            JavaPairRDD <Edge, Iterable <Integer>> invUpdates = tSet
                    .filter(kv -> kv._2[0] < minSup)
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
            tSet = tSet.filter(kv -> kv._2[0] >= minSup).leftOuterJoin(invUpdates)
                    .mapValues(values -> {
                        org.apache.spark.api.java.Optional <Iterable <Integer>> invalidUpdate = values._2;
                        int[] set = values._1;

                        // If no invalid vertex is present for the current edge then return the set value.
                        if (!invalidUpdate.isPresent()) {
                            return set;
                        }

                        IntSet iSet = new IntOpenHashSet();
                        for (int v : invalidUpdate.get()) {
                            iSet.add(v);
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
