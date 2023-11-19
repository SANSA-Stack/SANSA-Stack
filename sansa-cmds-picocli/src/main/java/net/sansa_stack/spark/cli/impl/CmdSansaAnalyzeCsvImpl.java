package net.sansa_stack.spark.cli.impl;

import java.util.ArrayList;
import java.util.List;

import net.sansa_stack.spark.cli.util.SansaCmdUtils;
import net.sansa_stack.spark.cli.util.SimpleSparkCmdTemplate;
import net.sansa_stack.spark.io.csv.input.CsvRowMapperFactories;
import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.commons.model.csvw.univocity.UnivocityCsvwConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFFormat;
import org.apache.spark.api.java.JavaRDD;

import net.sansa_stack.spark.cli.cmd.CmdSansaAnalyzeCsv;
import net.sansa_stack.spark.io.csv.input.CsvDataSources;
import net.sansa_stack.spark.io.rdf.input.api.HadoopInputData;
import net.sansa_stack.spark.io.rdf.input.api.InputFormatUtils;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSources;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;

public class CmdSansaAnalyzeCsvImpl {

    public static int run(CmdSansaAnalyzeCsv cmd) throws Exception {

        RddRdfWriterFactory rddRdfWriterFactory = SansaCmdUtils.configureWriter(cmd.outputConfig);
        rddRdfWriterFactory.setUseElephas(true);
        rddRdfWriterFactory.getPostProcessingSettings().copyFrom(cmd.postProcessConfig);

        if (rddRdfWriterFactory.getOutputFormat() == null) {
            rddRdfWriterFactory.setOutputFormat(RDFFormat.TURTLE_BLOCKS);
        }
        rddRdfWriterFactory.validate();

        new SimpleSparkCmdTemplate<>("Sansa Analyze Data in Splits", cmd.inputFiles) {
            @Override
            protected void process() throws Exception {
                List<JavaRDD<Model>> rdds = new ArrayList<>();
                for (String  inputPath : inputFiles) {
                    Configuration conf = new Configuration(sparkContext.hadoopConfiguration());

                    UnivocityCsvwConf univocityConf = new UnivocityCsvwConf();
                    DialectMutable csvCliOptions = cmd.csvOptions;
                    csvCliOptions.copyInto(univocityConf.getDialect(), false);
                    univocityConf.setTabs(cmd.tabs);

                    HadoopInputData<?, ?, ?> baseInputFormatData = CsvDataSources.configureHadoop(conf, inputPath, univocityConf, cmd.columnNamingSchemes, CsvRowMapperFactories::rowMapperFactoryBinding);
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
