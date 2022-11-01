package net.sansa_stack.spark.cli.main;

import com.google.common.base.Stopwatch;
import net.sansa_stack.hadoop.format.jena.trig.RecordReaderRdfTrigDataset;
import org.aksw.commons.util.stream.SequentialGroupBySpec;
import org.aksw.commons.util.stream.StreamOperatorSequentialGroupBy;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.aksw.jenax.sparql.query.rx.StreamUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.riot.system.AsyncParserBuilder;
import org.apache.jena.sparql.core.Quad;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MainCliSansaPlayground {
    public static void main(String[] args) {
        Stopwatch sw = Stopwatch.createStarted();
        try (Stream<Quad> rawStream = AsyncParser.of("/home/raven/Datasets/lsq2/homologene.merged.lsq.v2.trig.bz2").streamQuads()) {

            RecordReaderRdfTrigDataset.AccumulatingDataset accumulating = new RecordReaderRdfTrigDataset.AccumulatingDataset();
            SequentialGroupBySpec<Quad, Node, DatasetOneNg> spec = SequentialGroupBySpec.create(
                    accumulating::classify,
                    (accNum, groupKey) -> accumulating.createAccumulator(groupKey),
                    accumulating::accumulate);

            Stream<DatasetOneNg> aggStream = StreamOperatorSequentialGroupBy.create(spec)
                    .transform(rawStream)
                    .map(e -> accumulating.accumulatedValue(e.getValue()));

            List<DatasetOneNg> list = aggStream.collect(Collectors.toList());
            Collections.sort(list, (a, b) -> a.getGraphName().compareTo(b.getGraphName()));

            // System.out.println(aggStream.count());
        }
        System.out.println(sw.elapsed(TimeUnit.SECONDS));
    }
}
