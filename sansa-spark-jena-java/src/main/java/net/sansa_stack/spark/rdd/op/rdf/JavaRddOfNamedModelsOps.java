package net.sansa_stack.spark.rdd.op.rdf;

import org.aksw.jena_sparql_api.rx.DatasetFactoryEx;
import org.aksw.jena_sparql_api.utils.model.ResourceInDataset;
import org.aksw.jena_sparql_api.utils.model.ResourceInDatasetImpl;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

/**
 * Operations on the RDD[(String, Model)] type.
 *
 * The naming "RddOfNamedModelOps" allows for future introduction of "RddOfNamedGraphOps" in case there is demand
 * for these operations on Jena's Graph level.
 */
public class JavaRddOfNamedModelsOps {

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
        return JavaRddOps.groupKeysAndReduceValues(rdd, distinct, sortGraphsByIri, numPartitions,
            (g1, g2) -> {
                g1.add(g2);
                return g1;
            });
    }

    /**
     * Map each (name, model) pair to a dataset with the same information
     *
     * @param rdd
     * @return
     */
    public static JavaRDD<Dataset> mapToDatasets(JavaPairRDD<String, Model> rdd) {
        return rdd.map(graphNameAndModel -> {
            Dataset r = DatasetFactory.create();
            r.addNamedModel(graphNameAndModel._1(), graphNameAndModel._2());
            return r;
        });
    }

    public static JavaRDD<Resource> mapToResources(JavaPairRDD<String, Model> rdd) {
        return rdd.map(graphNameAndModel -> {
            String graphName = graphNameAndModel._1();
            Model model = graphNameAndModel._2();

            Node node = NodeFactory.createURI(graphName);
            Resource r = model.asRDFNode(node).asResource();

            return r;
        });
    }

    public static JavaRDD<ResourceInDataset> mapToResourceInDataset(JavaPairRDD<String, Model> rdd) {
        return rdd.map(graphNameAndModel -> {
            String graphName = graphNameAndModel._1();
            Model model = graphNameAndModel._2();

            Node node = NodeFactory.createURI(graphName);

            Dataset dataset = DatasetFactoryEx.createInsertOrderPreservingDataset();
            dataset.addNamedModel(graphName, model);

            ResourceInDataset r = new ResourceInDatasetImpl(dataset, graphName, node);
            return r;
        });
    }

    /*
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
    */
}
