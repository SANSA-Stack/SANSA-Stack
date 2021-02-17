package net.sansa_stack.rdf.common.kyro.jena;

import com.google.common.base.Stopwatch;
import net.sansa_stack.rdf.common.kryo.jena.ThriftUtils;
import org.aksw.jena_sparql_api.utils.GraphUtils;
import org.aksw.jena_sparql_api.utils.NodeUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.NTripleReader;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.riot.out.NodeFormatterNT;
import org.apache.jena.riot.system.RiotLib;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class NodeSerializerPerformanceTest {

    public static Collection<Node> getNodes() {
        // Generate a sufficient number of nodes in order to avoid any caching issues
        Collection<Node> result = IntStream.range(0, 10000)
                .mapToObj(i -> "http://www.example.org/r" + i)
                .map(NodeFactory::createURI)
                .collect(Collectors.toList());
        return result;
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

    @Test
    public void testFramework() throws Exception {
        int runTimeInMs = 1000;
        int sleepTimeInMs = 100;
        double actualRatio = avgTimePerTask(runTimeInMs, () -> Collections.singleton("foo"), str -> {
            try {
                Thread.sleep(sleepTimeInMs);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        double expectedRatio = runTimeInMs / (double)sleepTimeInMs;

        System.out.println(String.format("Test tasks completed: %.3f tasks/second - expected ratio: %.3f", actualRatio, + expectedRatio));
    }


        @Test
    public void testRiotPerformance() throws Exception {
        Collection<Node> nodes = getNodes();
        // Warmup
        avgTimePerTask(3000, () -> nodes, NodeSerializerPerformanceTest::roundTripWithRiot);

        // Actual
        double ratio = avgTimePerTask(3000, () -> nodes, NodeSerializerPerformanceTest::roundTripWithRiot);
        System.out.println(String.format("Riot performance: %.3f roundtrips/second", ratio));
    }

    @Test
    public void testThriftPerformance() throws Exception {
        Collection<Node> nodes = getNodes();
        // Warmup
        avgTimePerTask(3000, () -> nodes, NodeSerializerPerformanceTest::roundTripWithThrift);

        // Actual
        double ratio = avgTimePerTask(3000, () -> nodes, NodeSerializerPerformanceTest::roundTripWithThrift);
        System.out.println(String.format("Thrift performance: %.3f roundtrips/second", ratio));

    }

    public static <T> double avgTimePerTask(long timeLimitInMs, Supplier<? extends Collection<T>> batches, Consumer<? super T> executor) throws InterruptedException {
        long taskCount = 0;
        Stopwatch sw = Stopwatch.createStarted();
        long elapsed;
        long timeLimitInNanos = timeLimitInMs * 1000l * 1000l;

        long batchCount = 0;
        while ((elapsed = sw.elapsed(TimeUnit.NANOSECONDS)) < timeLimitInNanos) {
            Collection<T> batch = batches.get();
            for (T task : batch) {
                executor.accept(task);
            }
//            Thread.sleep(100);
            taskCount += batch.size();
            ++batchCount;
        }

//        System.out.println(batchCount);
        double result = taskCount / ((double)elapsed * 1E-9);
        return result;
    }
}
