package ir.ac.sbu.sbm;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class KCore {

    public static JavaPairRDD <Integer, int[]> find(final int k, JavaPairRDD <Integer, int[]> neighbors,
                                                    int iterations) {
        int numPartitions = neighbors.getNumPartitions();

        for (int iter = 0; iter < iterations; iter++) {
            long t1 = System.currentTimeMillis();
            if ((iter + 1) % 50 == 0)
                neighbors.checkpoint();

            JavaPairRDD <Integer, int[]> invalids = neighbors.filter(nl -> nl._2.length < k).cache();
            long count = invalids.count();
            long t2 = System.currentTimeMillis();
            long duration = t2 - t1;
            System.out.println("K-core, invalids: " + count + ", duration: " + duration);

            if (count == 0)
                break;

            iterations = iter + 1;
            JavaPairRDD <Integer, Iterable <Integer>> invUpdate = invalids
                    .flatMapToPair(nl -> {
                        List <Tuple2 <Integer, Integer>> out = new ArrayList <>(nl._2.length);

                        for (int v : nl._2) {
                            out.add(new Tuple2 <>(v, nl._1));
                        }
                        return out.iterator();
                    }).groupByKey(numPartitions);

            neighbors = neighbors.filter(nl -> nl._2.length >= k)
                    .leftOuterJoin(invUpdate)
                    .mapValues(value -> {
                        if (!value._2.isPresent())
                            return value._1;

                        IntSet invSet = new IntOpenHashSet();
                        for (int inv : value._2.get()) {
                            invSet.add(inv);
                        }

                        IntList nSet = new IntArrayList();
                        for (int v : value._1) {
                            if (invSet.contains(v))
                                continue;
                            nSet.add(v);
                        }

                        return nSet.toIntArray();
                    }).cache();
        }
        return neighbors.repartition(numPartitions).persist(StorageLevel.MEMORY_AND_DISK());
    }
}
