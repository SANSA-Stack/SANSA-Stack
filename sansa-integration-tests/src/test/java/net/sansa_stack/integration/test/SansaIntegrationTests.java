package net.sansa_stack.integration.test;

import com.google.common.io.ByteSource;
import org.aksw.commons.util.exception.ExceptionUtilsAksw;
import org.aksw.jena_sparql_api.delay.extra.Delayer;
import org.aksw.jena_sparql_api.delay.extra.DelayerDefault;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.spark.deploy.SparkSubmit;
import org.junit.*;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class SansaIntegrationTests {

    protected Map<String, String> env;

    protected Path bundleFolder;
    protected DockerComposeContainer environment;

    protected Path stackJarBundleFolder;
    protected Path exampleJarBundleFolder;

    protected String sparkMasterUrl;


    /**
     * The name of an environment variable that points to the folder where to look for the jar bundle
     */
    public String STACK_JAR_BUNDLE_FOLDER_KEY = "stackJarBundleFolder";
    public String EXAMPLE_JAR_BUNDLE_FOLDER_KEY = "exampleJarBundleFolder";

    @BeforeClass
    public static void beforeClass() throws IOException {
        // Files.createDirectories(Paths.get(StandardSystemProperty.JAVA_IO_TMPDIR.value()).resolve("spark-events"));
    }

    @Before
    public void before() {
        env = System.getenv();
        stackJarBundleFolder = Paths.get(env.getOrDefault(STACK_JAR_BUNDLE_FOLDER_KEY, "../sansa-stack/sansa-stack-spark/target/"));
        exampleJarBundleFolder = Paths.get(env.getOrDefault(EXAMPLE_JAR_BUNDLE_FOLDER_KEY, "../sansa-stack/sansa-stack-spark/target/"));

        sparkMasterUrl = "spark://localhost:7077";

        environment =
                new DockerComposeContainer(new File("src/test/resources/docker-compose.yml"))
                        .withExposedService("spark-master", 8080)
                        // .withExposedService("spark-master", 7531)
                        .withExposedService("spark-master", 7077);
//                        .withExposedService("elasticsearch_1", ELASTICSEARCH_PORT);

        environment.start();
    }


    @After
    public void after() {
        environment.stop();
    }

    // @Test
    public void test() throws Exception {

        System.out.println("Started");
        ByteSource bs = new ByteSource() {
            @Override
            public InputStream openStream() throws IOException {
                return new URL("http://localhost:8080").openStream();
            }
        };

        System.out.println("READ: " + bs.asCharSource(StandardCharsets.UTF_8).read());
    }

    @Test
    public void testSparqlifySubmit() throws Exception {
        String jar = IOUtils.findLatestFile(stackJarBundleFolder, "*jar-with-dependencies*").toAbsolutePath().toString();
        String sparklifyUrl = "http://localhost:7531/sparql";

        String[] args = new String[] {
                // "--class", "net.sansa_stack.examples.spark.query.Sparklify",
                "--class", "net.sansa_stack.query.spark.sparqlify.server.MainSansaSparqlServer",
                "--master", sparkMasterUrl,
                "--num-executors", "2",
                "--executor-memory", "1G",
                "--executor-cores", "2",
                "--conf", "spark.eventLog.enabled=true",
//                "--conf", "spark.eventLog.dir=hdfs://qrowd3:8020/shared/spark-logs"
                jar,
                "-i", "rdf.nt"
        };

        new Thread(() -> {
            // System.out.println("Submitting");
            SparkSubmit.main(args);
            // System.out.println("Done");
        }).start();

        Delayer delayer = DelayerDefault.createFromNow(1000);
        long resultSetSize = -1;
        for (int i = 0; i < 120; ++i) {
            //SparqlQueryConnectionWithReconnect.create(() -> RDFConnectionFactory.connect(sparklfyUrl));
            System.out.println("Testing");
            try (RDFConnection conn = RDFConnectionFactory.connect(sparklifyUrl)) {
                resultSetSize = ResultSetFormatter.consume(conn.query("SELECT * { ?s ?p ?o }").execSelect());
                // System.out.println("Count: " + resultSetSize);
                break;
            } catch(Exception e) {
                System.out.println(ExceptionUtils.getRootCauseMessage(e));
                ExceptionUtilsAksw.rethrowUnless(e,
                        ExceptionUtilsAksw::isConnectionRefusedException,
                        ExceptionUtilsAksw::isBrokenPipeException);
            }

            try {
                delayer.doDelay();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        Assert.assertEquals(10, resultSetSize);
    }

}
