package net.sansa_stack.rdf.spark.rdd.op;

import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class JavaRddOfResourcesOps {
    /**
     * Map IRI resources to a named model.
     * Any mapping of non-IRI resources will fail.
     *
     * @author Claus Stadler 2021-10-07
     */
    public static JavaPairRDD<String, Model> mapToNamedModels(JavaRDD<Resource> rdd) {
        return rdd.mapToPair(r -> new Tuple2<>(r.getURI(), r.getModel()));
    }

    /**
     * Map every IRI resource to a dataset having a single named graph matching the IRI.
     * Any mapping of non-IRI resources will fail.
     *
     * @author Claus Stadler 2021-10-07
     */
    public static JavaRDD<Dataset> mapToDatasets(JavaRDD<Resource> rdd) {
        return JavaRddOfNamedModelsOps.mapToDatasets(mapToNamedModels(rdd));
    }
}
