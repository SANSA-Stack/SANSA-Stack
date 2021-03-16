package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.riot.system.RiotLib;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/* The benchmark shouldn't be a unit test but it is simple doing it that way */
@Ignore
public class NodeSerializerPerformanceTest {

    private static final Kryo kryo = new Kryo();

    static {
        JenaKryoRegistratorLib.registerNodeSerializers(kryo, new GenericNodeSerializerCustom());
    }

    public static Collection<Node> getNodes() {
        // Generate a sufficient number of nodes in order to avoid any caching issues
        Collection<Node> result = IntStream.range(0, 10000)
                .mapToObj(i -> "http://www.example.org/r" + i)
                .map(NodeFactory::createURI)
                .collect(Collectors.toList());
        return result;
    }

    public static void roundTripWithCustomFormat(Node expected) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        kryo.writeClassAndObject(output, expected);
        output.flush();
        output.close();
        byte[] bytes = out.toByteArray();
        Node actual = (Node)kryo.readClassAndObject(new Input(new ByteArrayInputStream(bytes)));
        Assert.assertEquals(expected, actual);
    }

    public static void roundTripWithThrift(Node expected) {
        byte[] bytes = ThriftUtils.writeNode(expected, false);
        Node actual = ThriftUtils.readNode(bytes);
        Assert.assertEquals(expected, actual);
    }

    public static void roundTripWithRiot(Node expected) {
        byte[] bytes = NodeFmtLib.str(expected).getBytes();
        Node actual = RiotLib.parse(new String(bytes, StandardCharsets.UTF_8));
        Assert.assertEquals(expected, actual);
    }

    /** Test the little benchmark framework itself for whether the obtained results are sane */
    @Test
    public void sanityCheckFramework() throws Exception {
        int runTimeInMs = 1000;
        int sleepTimeInMs = 20;
        double actualRatio = avgTimePerTask(runTimeInMs, () -> Collections.singleton("foo"), str -> {
            try {
                Thread.sleep(sleepTimeInMs);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        double expectedRatio = runTimeInMs / (double) sleepTimeInMs;

        System.out.printf("Test tasks completed: %.3f tasks/second - expected ratio: %.3f%n", actualRatio, expectedRatio);
    }


    @Test
    public void testRiotPerformance() throws Exception {
        Collection<Node> nodes = getNodes();
        // Warmup
        avgTimePerTask(3000, () -> nodes, NodeSerializerPerformanceTest::roundTripWithRiot);

        // Actual
        double ratio = avgTimePerTask(3000, () -> nodes, NodeSerializerPerformanceTest::roundTripWithRiot);
        System.out.printf("Riot performance: %.3f roundtrips/second%n", ratio);
    }

    @Test
    public void testThriftPerformance() {
        Collection<Node> nodes = getNodes();
        // Warmup
        avgTimePerTask(3000, () -> nodes, NodeSerializerPerformanceTest::roundTripWithThrift);

        // Actual
        double ratio = avgTimePerTask(3000, () -> nodes, NodeSerializerPerformanceTest::roundTripWithThrift);
        System.out.printf("Thrift performance: %.3f roundtrips/second%n", ratio);
    }

    @Test
    public void testCustomPerformance() {
        Collection<Node> nodes = getNodes();
        // Warmup
        avgTimePerTask(3000, () -> nodes, NodeSerializerPerformanceTest::roundTripWithCustomFormat);

        // Actual
        double ratio = avgTimePerTask(3000, () -> nodes, NodeSerializerPerformanceTest::roundTripWithCustomFormat);
        System.out.printf("Custom performance: %.3f roundtrips/second%n", ratio);
    }
    public static <T> double avgTimePerTask(long timeLimitInMs, Supplier<? extends Collection<T>> batches, Consumer<? super T> executor) {
        long taskCount = 0;
        StopWatch sw = StopWatch.createStarted();
        long timeLimitInNanos = timeLimitInMs * 1000L * 1000L;

        // long batchCount = 0;
        long elapsed;
        while ((elapsed = sw.getTime(TimeUnit.NANOSECONDS)) < timeLimitInNanos) {
            Collection<T> batch = batches.get();
            for (T task : batch) {
                executor.accept(task);
            }
            taskCount += batch.size();
            // ++batchCount;
        }

//        System.out.println(batchCount);
        double result = taskCount / ((double) elapsed * 1E-9);
        return result;
    }
}
