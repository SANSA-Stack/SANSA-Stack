package net.sansa_stack.query.spark.engine;

import com.github.jsonldjava.shaded.com.google.common.base.Preconditions;
import net.sansa_stack.spark.io.csv.input.CsvDataSources;
import net.sansa_stack.spark.io.csv.input.CsvRowMapperFactories;
import net.sansa_stack.spark.io.json.input.JsonDataSources;
import net.sansa_stack.spark.io.rdf.input.api.HadoopInputData;
import net.sansa_stack.spark.io.rdf.input.api.InputFormatUtils;
import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.commons.model.csvw.domain.impl.DialectMutableImpl;
import org.aksw.commons.model.csvw.univocity.UnivocityCsvwConf;
import org.aksw.commons.model.csvw.univocity.UnivocityParserFactory;
import org.aksw.jena_sparql_api.sparql.ext.url.JenaUrlUtils;
import org.aksw.jenax.arq.util.security.ArqSecurity;
import org.aksw.jenax.model.csvw.domain.api.Dialect;
import org.aksw.jenax.model.csvw.domain.api.Table;
import org.aksw.rml.model.LogicalSource;
import org.aksw.rml.model.QlTerms;
import org.aksw.rml.rso.model.SourceOutput;
import org.apache.hadoop.io.LongWritable;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Function;

public class RmlSourcesSpark {

//    public static QueryIterator parseCsvAsJson(LogicalSource logicalSource, Binding parentBinding, ExecutionContext execCxt) {
//
//    }

//    public static QueryIterator parseCsvAsJson(LogicalSource logicalSource, Var outVar, Binding parentBinding, ExecutionContext execCxt) {
//
//        Stream<JsonObject> stream = parseCsvAsJson(logicalSource, execCxt);
//        return QueryExecUtils.fromStream(stream, outVar, parentBinding, execCxt, RDFDatatypeJson::jsonToNode);
//    }

    public static JavaRDD<Binding> processSource(JavaSparkContext sc, LogicalSource logicalSource, Binding parentBinding, ExecutionContext execCxt) {
        Map<String, RmlSourceProcessor> registry = new HashMap<>();
        registry.put(QlTerms.CSV, RmlSourcesSpark::processSourceAsCsv);
        registry.put(QlTerms.JSONPath, RmlSourcesSpark::processSourceAsJson);
        // registry.put(QlTerms.JSONPath, RmlSourcesSpark::processSourceAsJson);
        // registry.put(QlTerms.XPath, RmlSourcesSpark::processSourceAsXml);

        String iri = logicalSource.getReferenceFormulationIri();
        Preconditions.checkArgument(iri != null, "Reference formulation not specified on source. " + logicalSource);

        RmlSourceProcessor processor = registry.get(iri);
        Preconditions.checkArgument(processor != null, "No processor found for reference formulation: " + iri);

        JavaRDD<Binding> result = processor.eval(sc, logicalSource, parentBinding, execCxt);
        return result;
    }

    public static JavaRDD<Binding> processSourceAsJson(JavaSparkContext sc, LogicalSource logicalSource, Binding parentBinding, ExecutionContext execCxt) {
        String source = logicalSource.getSourceAsString();
        SourceOutput output = logicalSource.as(SourceOutput.class);

        Var outVar = output.getOutputVar();
        String iterator = logicalSource.getIterator();

        if (iterator != null) {
            // Only support *
            if (!iterator.equals("$.[*]")) {
                throw new RuntimeException("Interpretation of JSON RML sources in a SPARK environment only supports the iterator '$.[*]'. This is also the default if the rml:iterator is omitted.");
            }
        }

        // TODO Make configurable
        int probeCount = 10;
        JavaRDD<Binding> result = JsonDataSources.createRddFromJson(sc, source, probeCount, outVar);

        return result;
    }

    /*
    public static JavaRDD<Binding> processSourceAsXml(JavaSparkContext sc, LogicalSource logicalSource, Binding parentBinding, ExecutionContext execCxt) {
        String source = logicalSource.getSource();
        SourceOutput output = logicalSource.as(SourceOutput.class);

        Var outVar = output.getOutputVar();
        String iterator = logicalSource.getIterator();
        Preconditions.checkArgument(iterator != null, "rml:iterator (an XPath expresion string) must always be specified for XML sources");

        NodeValue xmlNv;
        try {
            xmlNv = JenaXmlUtils.resolve(NodeValue.makeString(source), execCxt);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        XPathFactory xPathFactory = XPathFactory.newInstance();
        QueryIterator result = JenaXmlUtils.evalXPath(xPathFactory, parentBinding, execCxt,
                xmlNv.asNode(), NodeFactory.createLiteral(iterator), outVar);

        return result;
    }
    */



    public static JavaRDD<Binding> processSourceAsCsv(JavaSparkContext sc, LogicalSource logicalSource, Binding parentBinding, ExecutionContext execCxt) {

        SourceOutput output = logicalSource.as(SourceOutput.class);

        Var[] headerVars = null;
        // Try to get the outputs as an RDF list (may raise an exception)
        try {
            List<Var> headerVarList = output.getOutputVars();
            headerVars = headerVarList == null ? null : headerVarList.toArray(new Var[0]);
        } catch (Throwable e) {
            // Ignore
        }
        Var[] finalHeaderVars = headerVars;

        Var jsonVar = output.getOutputVar();

        if (jsonVar == null && headerVars == null) {
            throw new RuntimeException("No output specified");
        }

        String sourceDoc;
        String[] nullValues = null;
        RDFNode source = logicalSource.getSource();
        DialectMutable effectiveDialect = new DialectMutableImpl();
        if (source.isLiteral()) {
            sourceDoc = logicalSource.getSourceAsString();
        } else {
            Table csvwtSource = source.as(Table.class);
            Dialect dialect = csvwtSource.getDialect();
            if (dialect != null) {
                dialect.copyInto(effectiveDialect, false);
            }
            Set<String> nullSet = csvwtSource.getNull();
            if (nullSet != null && !nullSet.isEmpty()) {
                nullValues = nullSet.toArray(new String[0]);
            }
            sourceDoc = csvwtSource.getUrl();
        }
        Callable<InputStream> inSupp = () -> JenaUrlUtils.openInputStream(NodeValue.makeString(sourceDoc), execCxt);

        UnivocityCsvwConf csvConf = new UnivocityCsvwConf(effectiveDialect, nullValues);

        boolean jsonMode = finalHeaderVars == null;
        Function<String[][], Function<String[], Binding>> rowMapperFactory;
        if (jsonMode) {
            rowMapperFactory = CsvRowMapperFactories.rowMapperFactoryJson(null, jsonVar, CsvRowMapperFactories::rowToJson);
        } else {
            rowMapperFactory = CsvRowMapperFactories.rowMapperFactoryBinding(finalHeaderVars);
        }

        HadoopInputData<LongWritable, String[], JavaRDD<Binding>> hadoopInputFormat = CsvDataSources.configureHadoop(
                sc.hadoopConfiguration(), sourceDoc, csvConf, Arrays.asList("row"), rowMapperFactory);
        JavaRDD<Binding> result = InputFormatUtils.createRdd(sc, hadoopInputFormat);
        return result;
    }

    public static void main(String[] args) {
        try (QueryExec qe = QueryExec.newBuilder()
                .graph(GraphFactory.createDefaultGraph())
                .set(ArqSecurity.symAllowFileAccess, true)
                .query(String.join("\n",
                        "PREFIX rml: <http://semweb.mmlab.be/ns/rml#>",
                        "PREFIX ql: <http://semweb.mmlab.be/ns/ql#>",
                        "PREFIX fno: <https://w3id.org/function/ontology#>",
                        "SELECT * {",
                        "  SERVICE <rml.source:> {[",
                        "    rml:source '/home/raven/Repositories/coypu-data-sources/world_bank/target/clean/Metadata_Indicator_API_9_DS2_en_csv_v2_4775410.csv' ;",
                        "    rml:referenceFormulation ql:CSV ;",
                        "    fno:returns (?x ?y)",
                        "  ]}",
                        "} LIMIT 3"
                ))
                .build()) {
            System.out.println(ResultSetFormatter.asText(ResultSet.adapt(qe.select())));
        }

        try (QueryExec qe = QueryExec.newBuilder()
                .graph(GraphFactory.createDefaultGraph())
                .set(ArqSecurity.symAllowFileAccess, true)
                .query(String.join("\n",
                        "PREFIX rml: <http://semweb.mmlab.be/ns/rml#>",
                        "PREFIX ql: <http://semweb.mmlab.be/ns/ql#>",
                        "PREFIX fno: <https://w3id.org/function/ontology#>",
                        "SELECT * {",
                        "  SERVICE <rml.source:> {[",
                        "    rml:source '/home/raven/Projects/Eclipse/sansa-stack-parent/pom.xml' ;",
                        "    rml:referenceFormulation ql:XPath ;",
                        "    rml:iterator '//:dependency' ;",
                        "    fno:returns ?x",
                        "  ]}",
                        "} LIMIT 3"
                ))
                .build()) {
            System.out.println(ResultSetFormatter.asText(ResultSet.adapt(qe.select())));
        }

        try (QueryExec qe = QueryExec.newBuilder()
                .graph(GraphFactory.createDefaultGraph())
                .set(ArqSecurity.symAllowFileAccess, true)
                .query(String.join("\n",
                        "PREFIX rml: <http://semweb.mmlab.be/ns/rml#>",
                        "PREFIX ql: <http://semweb.mmlab.be/ns/ql#>",
                        "PREFIX fno: <https://w3id.org/function/ontology#>",
                        "SELECT * {",
                        "  SERVICE <rml.source:> {[",
                        "    rml:source '/home/raven/Repositories/aksw-jena/jena-arq/testing/ResultSet/rs-datatype-string.srj' ;",
                        "    rml:referenceFormulation ql:JSONPath ;",
                        "    rml:iterator '$..type' ;",
                        "    fno:returns ?x",
                        "  ]}",
                        "} LIMIT 3"
                ))
                .build()) {
            System.out.println(ResultSetFormatter.asText(ResultSet.adapt(qe.select())));
        }
    }
}
