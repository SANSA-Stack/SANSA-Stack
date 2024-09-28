package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.spark.cli.util.SansaCmdUtils;
import net.sansa_stack.spark.cli.util.SimpleSparkCmdRdfTemplate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;

import com.google.common.base.Preconditions;

import net.sansa_stack.spark.cli.cmd.CmdSansaAnalyzeRdf;
import net.sansa_stack.spark.io.rdf.input.api.HadoopInputData;
import net.sansa_stack.spark.io.rdf.input.api.InputFormatUtils;
import net.sansa_stack.spark.io.rdf.input.api.RddRdfLoader;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSources;
import net.sansa_stack.spark.io.rdf.input.impl.RddRdfLoaderRegistryImpl;
import net.sansa_stack.spark.io.rdf.input.impl.RdfSourceFromResourceImpl;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;

public class CmdSansaAnalyzeRdfImpl {

    public static int run(CmdSansaAnalyzeRdf cmd) throws Exception {

        RddRdfWriterFactory rddRdfWriterFactory = SansaCmdUtils.configureRdfWriter(cmd.outputConfig);
        // rddRdfWriterFactory.setUseElephas(true);
        rddRdfWriterFactory.getPostProcessingSettings().copyFrom(cmd.postProcessConfig);

        if (rddRdfWriterFactory.getOutputFormat() == null) {
            rddRdfWriterFactory.setOutputFormat(RDFFormat.TURTLE_BLOCKS);
        }
        rddRdfWriterFactory.validate();

        new SimpleSparkCmdRdfTemplate<>("Sansa Analyze Data in Splits", cmd.inputConfig, cmd.inputFiles) {
            @Override
            protected void process() throws Exception {
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
                    Path path = rdfSource.getPath();

                    @SuppressWarnings({ "rawtypes", "unchecked" })
                    HadoopInputData hid = new HadoopInputData(path.toString(), inputFormatClass, LongWritable.class, rdfLoader.getValueClass(), hc, null);
                    HadoopInputData<LongWritable, Resource, JavaRDD<Model>> wrappedHid = InputFormatUtils.wrapWithAnalyzer(hid);
                    JavaRDD<Model> rdd = InputFormatUtils.createRdd(sparkContext, wrappedHid);

                    RdfSource tgt = RdfSources.ofModels(rdd);
                    CmdSansaMapImpl.writeOutRdfSources(tgt, rddRdfWriterFactory);
/*
                    CmdSansaMapImpl.writeOutRdfSources(rdf);
                    List<Stats2> stats = rdd.collect().stream().map(r -> r.as(Stats2.class)).collect(Collectors.toList());

                    for (Stats2 stat : stats) {
                        RDFDataMgr.write(StdIo.openStdOut(), stat.getModel(), RDFFormat.TURTLE);
                    }
 */
                }
            }
        }.call();

        return 0; // exit code
    }

}
