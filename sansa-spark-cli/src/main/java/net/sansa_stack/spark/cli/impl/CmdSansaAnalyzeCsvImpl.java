package net.sansa_stack.spark.cli.impl;

import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFFormat;
import org.apache.spark.api.java.JavaRDD;

import net.sansa_stack.hadoop.format.univocity.conf.UnivocityHadoopConf;
import net.sansa_stack.spark.cli.cmd.CmdSansaAnalyzeCsv;
import net.sansa_stack.spark.io.csv.input.CsvDataSources;
import net.sansa_stack.spark.io.rdf.input.api.HadoopInputData;
import net.sansa_stack.spark.io.rdf.input.api.InputFormatUtils;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSources;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;

public class CmdSansaAnalyzeCsvImpl {

    public static int run(CmdSansaAnalyzeCsv cmd) throws Exception {

        RddRdfWriterFactory rddRdfWriterFactory = CmdUtils.configureWriter(cmd.outputConfig);
        rddRdfWriterFactory.setUseElephas(true);
        rddRdfWriterFactory.getPostProcessingSettings().copyFrom(cmd.postProcessConfig);

        if (rddRdfWriterFactory.getOutputFormat() == null) {
            rddRdfWriterFactory.setOutputFormat(RDFFormat.TURTLE_BLOCKS);
        }
        rddRdfWriterFactory.validate();

        new SimpleSparkCmdTemplate<>("Sansa Analyze Data in Splits", cmd.inputFiles) {
            @Override
            protected void process() throws Exception {

                for (String  inputPath : inputFiles) {
                    Configuration conf = new Configuration(sparkContext.hadoopConfiguration());

                    UnivocityHadoopConf univocityConf = new UnivocityHadoopConf();
                    DialectMutable csvCliOptions = cmd.csvOptions;
                    csvCliOptions.copyInto(univocityConf.getDialect(), false);
                    univocityConf.setTabs(cmd.tabs);

                    HadoopInputData<?, ?, ?> baseInputFormatData = CsvDataSources.configureHadoop(conf, inputPath, univocityConf, cmd.columnNamingSchemes);
                    HadoopInputData<LongWritable, Resource, JavaRDD<Model>> wrappedInputFormatData = InputFormatUtils.wrapWithAnalyzer(baseInputFormatData);
                    JavaRDD<Model> rdd = InputFormatUtils.createRdd(sparkContext, wrappedInputFormatData);

                    RdfSource tgt = RdfSources.ofModels(rdd);
                    CmdSansaMapImpl.writeOutRdfSources(tgt, rddRdfWriterFactory);
                }
            }
        }.call();

        return 0; // exit code
    }

}
