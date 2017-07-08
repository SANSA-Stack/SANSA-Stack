package net.sansa_stack.query.flink.playground;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.junit.Test;


public class TestCalciteJoin {
    public static class Triple {
        public String s;
        public String p;
        public String o;

        public Triple(String s, String p, String o) {
            super();
            this.s = s;
            this.p = p;
            this.o = o;
        }

    }

    public static class TestSchema {
        public final Triple[] rdf = {new Triple("s", "p", "o")};
    }


    @Test
    public void testCalciteJoin() throws Exception {
//    public static void main(String[] args) throws Exception {

        SchemaPlus rootSchema = Frameworks.createRootSchema(true);

//        SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
//        Table table =
        //        final TableFunction table = TableFunctionImpl.create(MAZE_METHOD);
//        schema.add("Maze", table);
//        final TableFunction table2 = TableFunctionImpl.create(SOLVE_METHOD);
//        schema.add("Solve", table2);
//        final String sql;

        //ReflectiveSchema.Factory.create(rootSchema, );

        rootSchema.add("s", new ReflectiveSchema(new TestSchema()));
//        System.out.println(rootSchema);

        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        // set defaultSchema
        configBuilder.defaultSchema(rootSchema);
        // build configuration
        FrameworkConfig frameworkConfig = configBuilder.build();

        // get parser config builder and modify set SQL case sensitive to false
        SqlParser.ConfigBuilder parserConfig = SqlParser.configBuilder(frameworkConfig.getParserConfig());
        parserConfig
            .setCaseSensitive(false)
            .setConfig(parserConfig.build());

        // get planner
        Planner planner = Frameworks.getPlanner(frameworkConfig);
        // parse SQL statement
        SqlNode sqlNode = planner.parse("SELECT * FROM \"s\".\"rdf\" \"a\", \"s\".\"rdf\" \"b\" WHERE \"a\".\"s\" = 5 and \"b\".\"s\" = 5");
        planner.validate(sqlNode);
        RelRoot relRoot = planner.rel(sqlNode);
        RelNode relNode = relRoot.project();
        System.out.println(RelOptUtil.toString(relNode));
    }

}
