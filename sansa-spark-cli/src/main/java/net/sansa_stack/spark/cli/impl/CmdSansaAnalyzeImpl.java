package net.sansa_stack.spark.cli.impl;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.aksw.commons.io.util.StdIo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;

import com.google.common.base.Preconditions;

import net.sansa_stack.hadoop.core.InputFormatStats;
import net.sansa_stack.hadoop.core.Stats2;
import net.sansa_stack.spark.cli.cmd.CmdSansaAnalyze;
import net.sansa_stack.spark.io.rdf.input.api.RddRdfLoader;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.impl.RddRdfLoaderRegistryImpl;
import net.sansa_stack.spark.io.rdf.input.impl.RdfSourceFromResourceImpl;

public class CmdSansaAnalyzeImpl {

    public static int run(CmdSansaAnalyze cmd) throws IOException {

        new SimpleSparkCmdTemplate<>("Sansa Analyze Data in Splits", cmd.inputConfig, cmd.inputFiles) {
            @Override
            protected void process() {
                for (RdfSource member : rdfSources.getMembers()) {
                    Preconditions.checkArgument(member instanceof RdfSourceFromResourceImpl, "Rdf Source must be an instance of " + RdfSourceFromResourceImpl.class);
                    RdfSourceFromResourceImpl rdfSource = (RdfSourceFromResourceImpl)member;
                    Lang lang = rdfSource.getLang();
                    Preconditions.checkNotNull(lang, "Could not detect an rdf language for rdf source" + rdfSource);

                    RddRdfLoader<?> rdfLoader = RDFLanguages.isQuads(lang)
                        ? RddRdfLoaderRegistryImpl.get().find(lang, Quad.class)
                        : RddRdfLoaderRegistryImpl.get().find(lang, Triple.class);

                    Class<? extends FileInputFormat<LongWritable, ?>> inputFormatClass = rdfLoader.getFileInputFormatClass();

                    Configuration hc = new Configuration(hadoopConfiguration);
                    String delegateClassName = inputFormatClass.getName();
                    hc.set("delegate", delegateClassName);

                    Path path = rdfSource.getPath();
                    JavaRDD<Resource> rdd = sparkContext.newAPIHadoopFile(path.toString(), InputFormatStats.class, LongWritable.class, Resource.class, hc)
                                .map(x -> x._2);

                    List<Stats2> stats = rdd.collect().stream().map(r -> r.as(Stats2.class)).collect(Collectors.toList());

                    for (Stats2 stat : stats) {
                        RDFDataMgr.write(StdIo.openStdOut(), stat.getModel(), RDFFormat.TURTLE);
                    }
                }
            }
        }.call();

        return 0; // exit code
    }

}