package net.sansa_stack.rdf.spark.rdd.op.java;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

/**
 * Operations on the RDD[(String, Model)] type.
 *
 * The naming "RddOfNamedModelOps" allows for future introduction of "RddOfNamedGraphOps" in case there is demand
 * for these operations on Jena's Graph level.
 */
public class NamedModelOpsRddJava {

    /**
     * Map each (name, model) pair to a dataset with the same information
     *
     * @param rdd
     * @return
     */
    public static JavaRDD<Dataset> mapToDatasets(JavaPairRDD<String, Model> rdd) {
        return rdd.map(graphNameAndmodel -> {
            Dataset r = DatasetFactory.create();
            r.addNamedModel(graphNameAndmodel._1(), graphNameAndmodel._2());
            return r;
        });
    }

    /**
     * Group and/or sort named models by their graph iri
     *
     * @param rdd
     * @param distinct        If false then models with the same key remain
     *                        separated otherwise they become merged
     * @param sortGraphsByIri Whether to apply sorting in addition to grouping
     * @param numPartitions   Number of partitions to use for sorting; only
     *                        applicable if sortGraphsByIri is true.
     * @return
     */
    public static <K> JavaPairRDD<K, Model> groupNamedModels(
            JavaPairRDD<K, Model> rdd,
            boolean distinct,
            boolean sortGraphsByIri,
            int numPartitions) {
        JavaPairRDD<K, Model> resultRdd = rdd;

        if (distinct) {
            resultRdd = resultRdd.reduceByKey((g1, g2) -> {
                g1.add(g2);
                return g1;
            });
        }

        if (numPartitions > 0) {
            if (sortGraphsByIri) {
                resultRdd = resultRdd.repartitionAndSortWithinPartitions(new HashPartitioner(numPartitions));
            } else {
                resultRdd = resultRdd.repartition(numPartitions);
            }
        }

        if (sortGraphsByIri) {
            resultRdd = resultRdd.sortByKey();
        }

        return resultRdd;
    }
}