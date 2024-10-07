package net.sansa_stack.query.spark.engine;

import com.google.common.base.Preconditions;
import com.zaxxer.hikari.HikariDataSource;
import net.sansa_stack.spark.io.csv.input.CsvDataSources;
import net.sansa_stack.spark.io.csv.input.CsvRowMapperFactories;
import net.sansa_stack.spark.io.json.input.JsonDataSources;
import net.sansa_stack.spark.io.rdf.input.api.HadoopInputData;
import net.sansa_stack.spark.io.rdf.input.api.InputFormatUtils;
import net.sansa_stack.spark.util.JavaSparkContextUtils;
import net.sf.jsqlparser.JSQLParserException;
import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.commons.model.csvw.domain.impl.DialectMutableImpl;
import org.aksw.commons.model.csvw.univocity.UnivocityCsvwConf;
import org.aksw.commons.sql.codec.api.SqlCodec;
import org.aksw.commons.sql.codec.util.SqlCodecUtils;
import org.aksw.commons.util.jdbc.ColumnsReference;
import org.aksw.commons.util.jdbc.Index;
import org.aksw.commons.util.jdbc.JdbcUtils;
import org.aksw.jena_sparql_api.rdf.collections.NodeMapper;
import org.aksw.jena_sparql_api.rdf.collections.NodeMapperFromTypeMapper;
import org.aksw.jena_sparql_api.sparql.ext.url.JenaUrlUtils;
import org.aksw.jenax.arq.util.security.ArqSecurity;
import org.aksw.jenax.model.csvw.domain.api.Dialect;
import org.aksw.jenax.model.csvw.domain.api.Table;
import org.aksw.jenax.model.d2rq.domain.api.D2rqDatabase;
import org.aksw.r2rml.sql.transform.JSqlUtils;
import org.aksw.rml.jena.service.D2rqHikariUtils;
import org.aksw.rml.jena.service.InitRmlService;
import org.aksw.rml.model.QlTerms;
import org.aksw.rml.rso.model.SourceOutput;
import org.aksw.rml.v2.io.RelativePathSource;
import org.aksw.rmltk.model.backbone.rml.ILogicalSource;
import org.aksw.rmltk.model.r2rml.LogicalTable;
import org.apache.hadoop.io.LongWritable;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import javax.sql.DataSource;
import java.io.InputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Statement;
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

    public static JavaRDD<Binding> processSource(JavaSparkContext sc, ILogicalSource logicalSource, Binding parentBinding, ExecutionContext execCxt) {
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

    public static JavaRDD<Binding> processSourceAsJson(JavaSparkContext sc, ILogicalSource logicalSource, Binding parentBinding, ExecutionContext execCxt) {
        SourceOutput output = logicalSource.as(SourceOutput.class);

        Var outVar = output.getOutputVar();
        String iterator = logicalSource.getIterator();

        if (iterator != null) {
            // Only support *
            if (!iterator.equals("$.[*]")) {
                throw new RuntimeException("Interpretation of JSON RML sources in a SPARK environment only supports the iterator '$.[*]'. This is also the default if the rml:iterator is omitted.");
            }
        }

        String sourceDoc;
        RDFNode source = logicalSource.getSource();
        if (source.isLiteral()) {
            sourceDoc = logicalSource.getSourceAsString();
        } else {
            // Try RML 2
            RelativePathSource rps = source.as(RelativePathSource.class);
            // if (rps.getPath() != null) {
            sourceDoc = rps.getPath();
            // }
        }

        // Resolve the source path against the mapping directory from the context (if present)
        Path mappingDirectory = InitRmlService.getMappingDirectory(execCxt.getContext(), false);
        if (mappingDirectory != null) {
            sourceDoc = mappingDirectory.resolve(sourceDoc).toString();
        }

        // TODO Make configurable
        int probeCount = 10;
        JavaRDD<Binding> result = JsonDataSources.createRddFromJson(sc, sourceDoc, probeCount, outVar);

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


    public static JavaRDD<Binding> processSourceAsCsv(JavaSparkContext sc, ILogicalSource logicalSource, Binding parentBinding, ExecutionContext execCxt) {
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

            // Try RML 2
            if (sourceDoc == null) {
                RelativePathSource rps = csvwtSource.as(RelativePathSource.class);
                if (rps.getPath() != null) {
                    sourceDoc = rps.getPath();
                }
            }
        }
        // Callable<InputStream> inSupp = () -> JenaUrlUtils.openInputStream(NodeValue.makeString(sourceDoc), execCxt);

        UnivocityCsvwConf csvConf = new UnivocityCsvwConf(effectiveDialect, nullValues);

        // Resolve the source path against the mapping directory from the context (if present)
        Path mappingDirectory = InitRmlService.getMappingDirectory(execCxt.getContext(), false);
        if (mappingDirectory != null) {
            sourceDoc = mappingDirectory.resolve(sourceDoc).toString();
        }

        boolean jsonMode = finalHeaderVars == null;
        Function<String[][], Function<String[], Binding>> rowMapperFactory;
        if (jsonMode) {
            rowMapperFactory = CsvRowMapperFactories.rowMapperFactoryJson(null, jsonVar, CsvRowMapperFactories::rowToJsonWithNulls);
        } else {
            rowMapperFactory = CsvRowMapperFactories.rowMapperFactoryBinding(finalHeaderVars);
        }

        HadoopInputData<LongWritable, String[], JavaRDD<Binding>> hadoopInputFormat = CsvDataSources.configureHadoop(
                sc.hadoopConfiguration(), sourceDoc, csvConf, Arrays.asList("row"), rowMapperFactory);
        JavaRDD<Binding> result = InputFormatUtils.createRdd(sc, hadoopInputFormat);
        return result;
    }

    /** Configure a hikari config from a d2rq model */
    public static DataFrameReader configure(DataFrameReader target, D2rqDatabase source) {
        String value;
        if ((value = source.getJdbcDriver()) != null) {
            target.option("driver", value);
        }
        if ((value = source.getJdbcDSN()) != null) {
            target = target.option("url", value);
        }
        if ((value = source.getUsername()) != null) {
            target = target.option("user", value);
        }
        if ((value = source.getPassword()) != null) {
            target = target.option("password", value);
        }
        return target;
    }

    public static class PartitionColumn {
        protected String columnName;
        protected Object minValue;
        protected Object maxValue;

        public PartitionColumn(String columnName, Object minValue, Object maxValue) {
            this.columnName = columnName;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        public String getColumnName() {
            return columnName;
        }

        public Object getMinValue() {
            return minValue;
        }

        public Object getMaxValue() {
            return maxValue;
        }
    }

    public static PartitionColumn autoDetectPartitionColumn(DataSource dataSource, net.sf.jsqlparser.schema.Table table) {
        SqlCodec sqlCodec = SqlCodecUtils.createSqlCodecForApacheSpark();

        String sqlQuery;
        try {

            Collection<Index> indexes;

            try (Connection conn = dataSource.getConnection()) {
                DatabaseMetaData dbmd = conn.getMetaData();
                String catalog = conn.getCatalog();
                String schemaName = table.getSchemaName();
                String tableName = table.getName();
                indexes = JdbcUtils.fetchIndexes(dbmd, catalog, schemaName, tableName, false).values();

                for (Index index : indexes) {
                    ColumnsReference columns = index.getColumns();
                    List<String> columnNames = columns.getColumnNames();

                    // TODO We should get column metadata about whether spark/java knows whether how to compare values

                    if (columns.getColumnNames().size() == 1) {
                        String columnName = columnNames.iterator().next();

                        try (Statement stmt = conn.createStatement()) {
                            try (java.sql.ResultSet rs = stmt.executeQuery("SELECT MIN(column_name), MAX(column_name) FROM " + tableName)) {
                                // Get the result
                                if (rs.next()) {
                                    int minValue = rs.getInt(1);
                                    int maxValue = rs.getInt(2);
                                    System.out.println("Column: " + columnName + ", Min: " + minValue + ", Max: " + maxValue);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static JavaRDD<Binding> processSourceAsRdb(JavaSparkContext sc, ILogicalSource logicalSource, Binding parentBinding, ExecutionContext execCxt) {
        SourceOutput output = logicalSource.as(SourceOutput.class);
        Var outVar = output.getOutputVar();

        // TODO Register data source with execCxt and reuse if present
        LogicalTable logicalTable = logicalSource.as(LogicalTable.class);
        D2rqDatabase dataSourceSpec = logicalSource.getSource().as(D2rqDatabase.class);

        SqlCodec sqlCodec = SqlCodecUtils.createSqlCodecForApacheSpark(); // SqlCodecUtils.createSqlCodecDefault();

        String rawTableName = logicalTable.asBaseTableOrView().getTableName();

        net.sf.jsqlparser.schema.Table table = null;
        try {
            table = JSqlUtils.parseTableName(rawTableName);
        } catch (JSQLParserException e) {
            throw new RuntimeException(e);
        }
        table = JSqlUtils.harmonizeTable(table, sqlCodec);
        String tableName = table.toString();

        SparkSession spark = JavaSparkContextUtils.getSession(sc);
        DataFrameReader reader = spark.read()
                .format("jdbc")
                .option("dbtable", tableName);

        if (logicalTable.qualifiesAsBaseTableOrView()) {
            PartitionColumn partitionColumn = null;
            try (HikariDataSource dataSource = D2rqHikariUtils.configureDataSource(dataSourceSpec)) {
                // TODO Computing the partition column needs to be done as an algebra aptimization just before query execution
                 partitionColumn = autoDetectPartitionColumn(dataSource, table);
            }

            reader = configure(reader, dataSourceSpec);
            if (partitionColumn != null) {
                reader = reader
                    .option("partitionColumn", partitionColumn.getColumnName())
                    .option("lowerBound", Objects.toString(partitionColumn.getMinValue(), null))
                    .option("upperBound", Objects.toString(partitionColumn.getMaxValue(), null))
                    .option("numPartitions", "10");
            }
        }

        Dataset<Row> df = reader.load();
        StructType schema = df.schema();
        JavaRDD<Row> rdd = df.toJavaRDD();

        JavaRDD<Binding> result = rdd.mapPartitions(it -> {
            // FIXME We should exploit the schema information to use specific node mappers that have
            //  no lookup overhead
            NodeMapper<?> nodeMapper = new NodeMapperFromTypeMapper(Object.class, TypeMapper.getInstance());
            return Iter.iter(it).map(row -> {
                Binding r = new BindingOverSparkRow(BindingFactory.root(), row, nodeMapper);
                // Set up a mapper from the row to RDF nodes
                // Binding r = BindingFactory.binding(parentBinding, outVar, new NodeValueBinding(bb).asNode());
                return r;
            });
        });
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
