package net.sansa_stack.spark.cli.impl;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.jena_sparql_api.rx.script.SparqlScriptProcessor;
import org.aksw.jenax.stmt.core.SparqlStmt;
import org.aksw.jenax.stmt.core.SparqlStmtQuery;
import org.aksw.jenax.stmt.core.SparqlStmtUpdate;
import org.aksw.jenax.stmt.util.SparqlStmtUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.jena.query.Query;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import net.sansa_stack.hadoop.format.univocity.conf.UnivocityHadoopConf;
import net.sansa_stack.spark.cli.cmd.CmdSansaTarql;
import net.sansa_stack.spark.io.csv.input.CsvDataSources;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSources;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfBindingsOps;

/**
 * Called from the Java class [[CmdSansaTarql]]
 */
public class CmdSansaTarqlImpl {
    private static final Logger logger = LoggerFactory.getLogger(CmdSansaTarqlImpl.class);

    public static Map<String, String> getOptionsFromIriFragment(String iri) {
        Map<String, String> result = Collections.emptyMap();
        int hashOffset = iri.lastIndexOf('#');
        if (hashOffset >= 0) {
            String str = iri.substring(hashOffset + 1);
            List<String> options = Arrays.asList(str.split(";"));
            result = options.stream()
                    .map(option -> {
                int sep = option.indexOf('=');
                Entry<String, String> r = sep >= 0
                        ? new SimpleEntry<>(option.substring(0, sep), option.substring(sep + 1))
                        : new SimpleEntry<>(option, "")
                        ;
                return r;
            })
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        }
        return result;
    }

    public static int run(CmdSansaTarql cmd) throws Exception {
        String queryFile = cmd.inputFiles.get(0);
        List<String> csvFiles = new ArrayList<>(cmd.inputFiles.subList(1, cmd.inputFiles.size()));

        RddRdfWriterFactory rddRdfWriterFactory = CmdUtils.configureWriter(cmd.outputConfig);

        // Parse the query against the configured prefixes - then reconfigure the prefixes
        // to those used in the query
        // The tradeoff is that prefixes generated in the query via the sparql IRI() function
        // may not be recognized this way (if built from string; IRI(STR(ns:), 'foo') will recognize 'ns:')
        PrefixMapping prefixes = rddRdfWriterFactory.getGlobalPrefixMapping();
        SparqlScriptProcessor processor = SparqlScriptProcessor.createPlain(prefixes, null);
        processor.process(queryFile);
        List<SparqlStmt> stmts = processor.getPlainSparqlStmts();
        //List<SparqlStmt> stmts = SparqlStmtMgr.loadSparqlStmts(queryFile, prefixes);

        // If no argument is given then check whether the first query's from clause can act as a source
        // Convention: The from clause of subsequent queries may refer to previously generated graphs

        if (stmts.isEmpty()) {
            throw new IllegalArgumentException("No queries for mapping detected");
        }

        if (csvFiles.isEmpty()) {
            SparqlStmt firstStmt = stmts.get(0);
            List<String> graphUris;
            if (firstStmt.isQuery()) {
                Query query = firstStmt.getQuery();
                graphUris = new ArrayList<>(query.getGraphURIs());
                query.getGraphURIs().clear();
            } else {
                throw new UnsupportedOperationException("Extracting CSV source from update request not implemented");
            }

            Preconditions.checkArgument(!graphUris.isEmpty(), "No CSV file specified and none could be derived from the first query");
            Preconditions.checkArgument(graphUris.size() == 1, "Either exactly one FROM clause expected or a CSV file needes to be provided");
            String csvUrl = graphUris.get(0);
            // csvUrl = csvUrl.replaceAll("^file://", ""); // Cut away the file protocol if present
            csvFiles.add(csvUrl);
        }

        PrefixMap usedPrefixes = PrefixMapFactory.create();
        for (SparqlStmt stmt : stmts) {
            // TODO optimizePrefixes should not modify in-place because it desyncs with the original string
            SparqlStmtUtils.optimizePrefixes(stmt);
            PrefixMapping pm = stmt.getPrefixMapping();
            if (pm != null) {
                usedPrefixes.putAll(pm);
            }
        }

        // Post processing because we need to update the original query strings such that
        //  they only make use of the optimized prefixes
        stmts = stmts.stream().map(stmt -> stmt.isQuery()
                    ? new SparqlStmtQuery(stmt.getQuery())
                    : new SparqlStmtUpdate(stmt.getUpdateRequest()))
            .collect(Collectors.toList());

        rddRdfWriterFactory.setGlobalPrefixMapping(usedPrefixes.getMapping());
        logger.info("Loaded statements " + stmts);

        RDFFormat fmt = rddRdfWriterFactory.getOutputFormat();
        if (fmt == null) {
            // TODO We also need to analyze the insert statements whether they make use of named graphs
            boolean mayProduceQuads = JavaRddOfBindingsOps.mayProduceQuads(stmts);

            fmt = mayProduceQuads ? RDFFormat.TRIG_BLOCKS : RDFFormat.TURTLE_BLOCKS;
            rddRdfWriterFactory.setOutputFormat(fmt);
        }
        if (cmd.ntriples) {
            Lang lang = fmt.getLang();
            fmt = RDFLanguages.isQuads(lang) ? RDFFormat.NQUADS : RDFFormat.NTRIPLES;
        }

        Lang outLang = fmt.getLang();

        rddRdfWriterFactory.setUseElephas(true);
        rddRdfWriterFactory.validate();
        rddRdfWriterFactory.getPostProcessingSettings().copyFrom(cmd.postProcessConfig);

        SparkSession sparkSession = CmdUtils.newDefaultSparkSessionBuilder()
                .appName("Sansa Tarql (" + cmd.inputFiles + ")")
                .getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        // Put the CSV options from the CLI into the hadoop context
        Configuration hadoopConf = javaSparkContext.hadoopConfiguration();
        UnivocityHadoopConf univocityConf = new UnivocityHadoopConf();

        DialectMutable csvCliOptions = cmd.csvOptions;
        csvCliOptions.copyInto(univocityConf.getDialect());
        univocityConf.setTabs(cmd.tabs);

        // FileInputFormatCsvUnivocity.setUnivocityConfig(hadoopConf, univocityConf);

        JavaRDD<Binding> initialRdd = CmdUtils.createUnionRdd(javaSparkContext, csvFiles,
                input -> CsvDataSources.createRddOfBindings(javaSparkContext, input,
                        univocityConf, cmd.columnNamingSchemes));

        boolean accumulationMode = cmd.accumulationMode;
        RdfSource rdfSource;
        if (RDFLanguages.isQuads(outLang)) {
            rdfSource = RdfSources.ofQuads(JavaRddOfBindingsOps.tarqlQuads(initialRdd, stmts, accumulationMode));
        } else if (RDFLanguages.isTriples(outLang)){
            rdfSource = RdfSources.ofTriples(JavaRddOfBindingsOps.tarqlTriples(initialRdd, stmts, accumulationMode));
        } else {
            throw new IllegalArgumentException("Unsupported output language: " + outLang);
        }

        CmdSansaMapImpl.writeOutRdfSources(rdfSource, rddRdfWriterFactory);

        return 0; // exit code
    }
}
