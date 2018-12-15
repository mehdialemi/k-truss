package ir.ac.sbu.sbm;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class EdgeLoader {

    public static JavaPairRDD<Integer, Integer> load(JavaSparkContext sc, String input) {
        return sc
                .textFile(input)
                .flatMapToPair(line -> {
                    if (line.startsWith("#"))
                        return Collections.emptyIterator();
                    String[] s = line.split("\\s+");

                    if (s.length != 2)
                        return Collections.emptyIterator();

                    int e1 = Integer.parseInt(s[0]);
                    int e2 = Integer.parseInt(s[1]);

                    if (e1 == e2)
                        return Collections.emptyIterator();

                    List<Tuple2<Integer, Integer>> list = new ArrayList<>();
                    list.add(new Tuple2 <>(e1, e2));
                    list.add(new Tuple2 <>(e2, e1));
                    return list.iterator();
                });
    }

    public static JavaPairRDD <Integer, int[]> createNeighbors(JavaPairRDD <Integer, Integer> edges) {
        int numPartitions = edges.getNumPartitions();
        return edges.groupByKey(numPartitions).mapToPair(t -> {
            IntSet set = new IntOpenHashSet();
            for (Integer v : t._2) {
                set.add(v.intValue());
            }
            return new Tuple2<>(t._1, set.toIntArray());
        }).cache();
    }
}
