package net.sansa_stack.integration.test;

import com.google.common.io.ByteSource;
import org.apache.spark.deploy.SparkSubmit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class SansaIntegrationTests {

    public static void sparkSubmit(String args[]) {
        SparkSubmit.main(args);
    }

    protected DockerComposeContainer environment;

    @Before
    public void before() {
        environment =
                new DockerComposeContainer(new File("src/test/resources/docker-compose.yml"))
                        .withExposedService("spark-master", 8080);
//                        .withExposedService("elasticsearch_1", ELASTICSEARCH_PORT);

        environment.start();
    }

    @After
    public void after() {
        environment.stop();
    }

    @Test
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
}
