package net.sansa_stack.spark.io.rdf.output;

import net.sansa_stack.hadoop.output.jena.base.OutputFormatRowSet;
import net.sansa_stack.hadoop.output.jena.base.RdfOutputUtils;
import net.sansa_stack.query.spark.api.domain.JavaResultSetSpark;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;

import java.util.List;

public class RddRowSetWriter {
    public static void write(JavaResultSetSpark rs, Path path, Lang lang) {
        write(rs.getRdd().rdd(), path, rs.getResultVars(), lang);
    }

    public static void write(RDD<Binding> rdd, Path path, List<Var> vars, Lang lang) {
        Configuration conf = RddRdfWriter2.buildBaseConfiguration(rdd);
        RdfOutputUtils.setVars(conf, vars);
        RdfOutputUtils.setLang(conf, lang);

        JavaPairRDD<Long, Binding> pairRdd = RddRdfWriter2.toPairRdd(rdd.toJavaRDD());
        pairRdd.saveAsNewAPIHadoopFile(path.toString(),
                Long.class,
                Binding.class,
                OutputFormatRowSet.class,
                conf);
    }
}