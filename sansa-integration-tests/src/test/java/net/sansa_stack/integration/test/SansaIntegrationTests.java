package net.sansa_stack.integration.test;

import com.google.common.io.ByteSource;
import org.aksw.commons.util.exception.ExceptionUtilsAksw;
import org.aksw.jena_sparql_api.delay.extra.Delayer;
import org.aksw.jena_sparql_api.delay.extra.DelayerDefault;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.riot.system.stream.StreamManager;
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
import java.util.function.Supplier;

public class SansaIntegrationTests {

    protected Map<String, String> env;

    protected Path bundleFolder;
    protected DockerComposeContainer environment;

    protected Path stackJarBundleFolder;
    protected Path exampleJarBundleFolder;

    protected String sparkMasterUrl;

    protected int sparkMasterPort;
    protected int sparkMasterWebUiPort;
    protected int sparkTestPort; // sansaTestPort?

    /**
     * The name of an environment variable that points to the folder where to look for the jar bundle
     */
    public String STACK_JAR_BUNDLE_FOLDER_KEY = "STACK_JAR_BUNDLE_FOLDER";
    public String EXAMPLE_JAR_BUNDLE_FOLDER_KEY = "EXAMPLE_JAR_BUNDLE_FOLDER";

    public String SPARK_MASTER_WEBUI_PORT_KEY = "SPARK_MASTER_WEBUI_PORT";
    public String SPARK_MASTER_PORT_KEY = "SPARK_MASTER_PORT";

    /** Some deployed spark apps open a port where they provide their service
     * (at present we expect only 1 additional port to be opened) */
    public String SPARK_TEST_PORT_KEY = "SPARK_TEST_PORT";

    @BeforeClass
    public static void beforeClass() throws IOException {
        // System.out.println(SansaIntegrationTests.class.getClassLoader().getResource("rdf.nt"));
        // throw new RuntimeException("bail out");
        // Files.createDirectories(Paths.get(StandardSystemProperty.JAVA_IO_TMPDIR.value()).resolve("spark-events"));
    }

    @Before
    public void before() {
        env = System.getenv();
        stackJarBundleFolder = Paths.get(env.getOrDefault(STACK_JAR_BUNDLE_FOLDER_KEY, "../sansa-stack/sansa-stack-spark/target/"));
        exampleJarBundleFolder = Paths.get(env.getOrDefault(EXAMPLE_JAR_BUNDLE_FOLDER_KEY, "../sansa-examples/sansa-examples-spark/target/"));


        sparkMasterWebUiPort = Integer.parseInt(env.getOrDefault(SPARK_MASTER_WEBUI_PORT_KEY, "7541"));
        sparkMasterPort = Integer.parseInt(env.getOrDefault(SPARK_MASTER_PORT_KEY, "7542"));
        sparkTestPort = Integer.parseInt(env.getOrDefault(SPARK_TEST_PORT_KEY, "7549"));

        sparkMasterUrl = "spark://localhost:" + sparkMasterPort;

        environment =
                new DockerComposeContainer(new File("src/test/resources/docker-compose.yml"))
                        .withExposedService("spark-master", 8080)
                        .withExposedService("spark-master", 7077);
//                        .withExposedService("spark-master", sparkMasterWebUiPort)
//                        .withExposedService("spark-master", sparkMasterPort);

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
        String jarBundlePath = IOUtils.findLatestFile(exampleJarBundleFolder, "*jar-with-dependencies*").toAbsolutePath().normalize().toString();
        String sparqlEndpointUrl = "http://localhost:" + sparkTestPort + "/sparql";
        // System.out.println("Jarbundle: " + jarBundlePath);

        String[] args = new String[] {
                 "--class", "net.sansa_stack.examples.spark.query.Sparklify",
                //"--class", "net.sansa_stack.examples.spark.query.OntopBasedSPARQLEngine",
                "--master", sparkMasterUrl,
                "--num-executors", "2",
                "--executor-memory", "1G",
                "--executor-cores", "2",
                "--conf", "spark.eventLog.enabled=true",
                jarBundlePath,
                "-i", "rdf.nt",
                "-r", "endpoint",
                "-p", Integer.toString(sparkTestPort)
        };

        Thread submitThread = new Thread(() -> { SparkSubmit.main(args); });
        submitThread.start();

        long resultSetSize = AwaitUtils.countResultBindings(
                sparqlEndpointUrl, "SELECT * { ?s ?p ?o }", submitThread::isAlive);

        // TODO Asserting the number of triples of a file called rdf.nt in the root of the classpath is fragile
        Assert.assertEquals(106, resultSetSize);
    }

    @Test
    public void testOntopSubmit() throws Exception {
        String jarBundlePath = IOUtils.findLatestFile(exampleJarBundleFolder, "*jar-with-dependencies*").toAbsolutePath().toString();
        String sparqlEndpointUrl = "http://localhost:" + sparkTestPort + "/sparql";

        String[] args = new String[] {
                // "--class", "net.sansa_stack.examples.spark.query.Sparklify",
                "--class", "net.sansa_stack.examples.spark.query.OntopBasedSPARQLEngine",
                "--master", sparkMasterUrl,
                "--num-executors", "2",
                "--executor-memory", "1G",
                "--executor-cores", "2",
                "--conf", "spark.eventLog.enabled=true",
                jarBundlePath,
                "-i", "rdf.nt",
                "-r", "endpoint",
                "-p", Integer.toString(sparkTestPort)

        };

        Thread submitThread = new Thread(() -> { SparkSubmit.main(args); });
        submitThread.start();

        long resultSetSize = AwaitUtils.countResultBindings(
                sparqlEndpointUrl, "SELECT * { ?s ?p ?o }", submitThread::isAlive);

        // TODO Asserting the number of triples of a file called rdf.nt in the root of the classpath is fragile
        Assert.assertEquals(106, resultSetSize);
    }

}
