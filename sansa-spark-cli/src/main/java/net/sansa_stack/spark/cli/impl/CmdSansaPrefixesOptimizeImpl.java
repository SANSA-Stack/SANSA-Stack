package net.sansa_stack.spark.cli.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.aksw.jenax.arq.analytics.NodeAnalytics;
import org.aksw.jenax.arq.util.quad.QuadUtils;
import org.aksw.jenax.arq.util.triple.TripleUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sansa_stack.spark.cli.cmd.CmdSansaMap;
import net.sansa_stack.spark.cli.cmd.CmdSansaPrefixesOptimize;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOps;


/**
 * Called from the Java class {@link CmdSansaMap}
 */
public class CmdSansaPrefixesOptimizeImpl {
  private static Logger logger = LoggerFactory.getLogger(CmdSansaPrefixesOptimizeImpl.class);

  public static int run(CmdSansaPrefixesOptimize cmd) throws Exception {

    new SimpleSparkCmdRdfTemplate<>("Sansa Prefixes Optimize", cmd.inputConfig, cmd.inputFiles) {
      @Override
      protected void process()  throws Exception {
        JavaRDD<Node> rdd;
        if (rdfSources.usesQuads()) {
          rdd = rdfSources.asQuads().toJavaRDD().flatMap(quad -> (QuadUtils.quadToList(quad).iterator()));
        } else {
          rdd = rdfSources.asTriples().toJavaRDD().flatMap(triple -> (TripleUtils.tripleToList(triple).iterator()));
        }

        // TODO Improve the usedPrefixes aggregator to provide usage counts
        // so we can do huffman-like encoding for the auto-generated names
        Set<String> usedPm = JavaRddOps.aggregateUsingJavaCollector(rdd,
                NodeAnalytics.usedPrefixes(cmd.targetSize).asCollector());
        List<String> list = new ArrayList(usedPm);
        Collections.sort(list);

        Model tmp = ModelFactory.createDefaultModel();
        for (int i = 0; i < list.size(); ++i) {
          tmp.setNsPrefix("ns" + i, list.get(i));
        }
        RDFDataMgr.write(System.out, tmp, RDFFormat.TURTLE_PRETTY);
      }
    }.call();

    return 0;
  }
}

