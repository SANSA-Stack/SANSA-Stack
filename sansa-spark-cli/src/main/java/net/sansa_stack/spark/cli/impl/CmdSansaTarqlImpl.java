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
import org.aksw.jena_sparql_api.sparql.ext.url.E_IriAsGiven.ExprTransformIriToIriAsGiven;
import org.aksw.jenax.stmt.core.SparqlStmt;
import org.aksw.jenax.stmt.core.SparqlStmtQuery;
import org.aksw.jenax.stmt.core.SparqlStmtUpdate;
import org.aksw.jenax.stmt.util.SparqlStmtUtils;
import org.apache.jena.query.Query;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sys.JenaSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

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

    static { JenaSystem.init(); }

    private static final Logger logger = LoggerFactory.getLogger(CmdSansaTarqlImpl.class);


    /** Parse tarql options as valid in an IRI hash fragment.
     *
     */
    public static Map<String, String> parseOptions(String str) {
        Map<String, String> result = Collections.emptyMap();
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
        return result;
    }

    // Mapping according to https://tarql.github.io/
    public static void configureDialectFromOptions(DialectMutable dialect, Map<String, String> map) {
        String str;

        if ((str = map.get("header")) != null) {
            if (str.equals("present")) {
                dialect.setHeaderRowCount(1l);
            } else if (str.equals("absent")) {
                dialect.setHeaderRowCount(0l);
            }
        }

        if ((str = map.get("delimiter")) != null) {
            Map<String, String> remap = ImmutableMap.<String, String>builder()
                    .put("comma", ",")
                    .put("tab", "\t")
                    .put("semicolon", ";")
                    .build();
            dialect.setDelimiter(remap.getOrDefault(str, str));
        }

        if ((str = map.get("quotechar")) != null) {
            Map<String, String> remap = ImmutableMap.<String, String>builder()
                    .put("none", "")
                    .put("singlequote", "'")
                    .put("doublequote", "\"")
                    .build();
            dialect.setQuoteChar(remap.getOrDefault(str, str));
        }

        if ((str = map.get("escapechar")) != null) {
            Map<String, String> remap = ImmutableMap.<String, String>builder()
                    .put("none", "")
                    .put("backslash", "\\")
                    .put("doublequote", "\"")
                    .build();
            dialect.setQuoteEscapeChar(remap.getOrDefault(str, str));
        }

        if ((str = map.get("encoding")) != null) {
            dialect.setEncoding(str);
        }
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

        UnivocityHadoopConf univocityConf = new UnivocityHadoopConf();

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
            String csvUrlWithHashFragment = graphUris.get(0);

            int hashPos = csvUrlWithHashFragment.lastIndexOf('#');
            String csvUrl = hashPos < 0 ? csvUrlWithHashFragment : csvUrlWithHashFragment.substring(0, hashPos);

            if (hashPos >= 0) {
                Map<String, String> options = parseOptions(csvUrlWithHashFragment.substring(hashPos + 1));
                configureDialectFromOptions(univocityConf.getDialect(), options);
            }

            // csvUrl = csvUrl.replaceAll("^file://", ""); // Cut away the file protocol if present
            csvFiles.add(csvUrl);
        }

        // CLI dialect options take precedence
        DialectMutable csvCliOptions = cmd.csvOptions;
        csvCliOptions.copyInto(univocityConf.getDialect(), false);
        univocityConf.setTabs(cmd.tabs);

        PrefixMap usedPrefixes = PrefixMapFactory.create();
        for (SparqlStmt stmt : stmts) {
            // TODO optimizePrefixes should not modify in-place because it desyncs with the original string
            SparqlStmtUtils.optimizePrefixes(stmt);
            PrefixMapping pm = stmt.getPrefixMapping();
            if (pm != null) {
                usedPrefixes.putAll(pm);
            }
        }

        if (cmd.useIriAsGiven) {
            stmts = stmts.stream()
                    .map(stmt -> SparqlStmtUtils.applyElementTransform(stmt, ExprTransformIriToIriAsGiven::transformElt))
                    .collect(Collectors.toList());
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
        // Configuration hadoopConf = javaSparkContext.hadoopConfiguration();

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
