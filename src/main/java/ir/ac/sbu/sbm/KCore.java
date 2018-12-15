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
                                                    int iterations, int finishPartition) {
        for (int iter = 0; iter < iterations; iter++) {
            long t1 = System.currentTimeMillis();
            if ((iter + 1) % 50 == 0)
                neighbors.checkpoint();

            JavaPairRDD <Integer, Iterable <Integer>> invUpdate = neighbors
                    .filter(nl -> nl._2.length < k)
                    .flatMapToPair(nl -> {
                        List <Tuple2 <Integer, Integer>> out = new ArrayList <>(nl._2.length);

                        for (int v : nl._2) {
                            out.add(new Tuple2 <>(v, nl._1));
                        }
                        return out.iterator();
                    }).groupByKey();

            long count = invUpdate.count();

            if (count == 0)
                break;

            long t2 = System.currentTimeMillis();
            long duration = t2 - t1;
            System.out.println(" K-core) iteration: " + iter + ", invUpdate count: " + count + ", duration: " + duration);

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

        return neighbors
                .repartition(finishPartition)
                .persist(StorageLevel.MEMORY_AND_DISK());
    }
}
