package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.spark.cli.cmd.CmdSansaAnalyzeJson;
import net.sansa_stack.spark.cli.util.SansaCmdUtils;
import net.sansa_stack.spark.cli.util.SimpleSparkCmdTemplate;
import net.sansa_stack.spark.io.json.input.JsonDataSources;
import net.sansa_stack.spark.io.rdf.input.api.HadoopInputData;
import net.sansa_stack.spark.io.rdf.input.api.InputFormatUtils;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSources;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFFormat;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;

public class CmdSansaAnalyzeJsonImpl {

    public static int run(CmdSansaAnalyzeJson cmd) throws Exception {

        RddRdfWriterFactory rddRdfWriterFactory = SansaCmdUtils.configureRdfWriter(cmd.outputConfig);
        // rddRdfWriterFactory.setUseElephas(true);
        rddRdfWriterFactory.getPostProcessingSettings().copyFrom(cmd.postProcessConfig);

        if (rddRdfWriterFactory.getOutputFormat() == null) {
            rddRdfWriterFactory.setOutputFormat(RDFFormat.TURTLE_BLOCKS);
        }
        rddRdfWriterFactory.validate();

        int probeCount = 10;

        new SimpleSparkCmdTemplate<>("Sansa Analyze Data in Splits", cmd.inputFiles) {
            @Override
            protected void process() throws Exception {
                List<JavaRDD<Model>> rdds = new ArrayList<>();
                for (String  inputPath : inputFiles) {
                    Configuration conf = new Configuration(sparkContext.hadoopConfiguration());
                    HadoopInputData<?, ?, ?> baseInputFormatData = JsonDataSources.probeJsonInputFormat(inputPath, conf, probeCount);
                    HadoopInputData<LongWritable, Resource, JavaRDD<Model>> wrappedInputFormatData = InputFormatUtils.wrapWithAnalyzer(baseInputFormatData);
                    JavaRDD<Model> rdd = InputFormatUtils.createRdd(sparkContext, wrappedInputFormatData);
                    rdds.add(rdd);
                }

                if (!rdds.isEmpty()) {
                    JavaRDD<Model> unionRdd = rdds.size() == 1
                            ? rdds.get(0)
                            : sparkContext.union(rdds.toArray(new JavaRDD[0]));

                    RdfSource tgt = RdfSources.ofModels(unionRdd);
                    CmdSansaMapImpl.writeOutRdfSources(tgt, rddRdfWriterFactory);
                }
            }
        }.call();

        return 0; // exit code
    }

}
