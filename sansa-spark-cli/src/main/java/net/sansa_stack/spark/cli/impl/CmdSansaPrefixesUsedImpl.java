package net.sansa_stack.spark.cli.impl;

import java.util.Map;

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
import net.sansa_stack.spark.cli.cmd.CmdSansaPrefixesUsed;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOps;


/**
 * Called from the Java class {@link CmdSansaMap}
 */
public class CmdSansaPrefixesUsedImpl {
  private static Logger logger = LoggerFactory.getLogger(CmdSansaPrefixesUsedImpl.class);

  public static int run(CmdSansaPrefixesUsed cmd) throws Exception {

    new SimpleSparkCmdRdfTemplate<>("Sansa Prefixes Used", cmd.inputConfig, cmd.inputFiles) {
      @Override
      protected void process()  throws Exception {
        Model model = rdfSources.peekDeclaredPrefixes();

        JavaRDD<Node> rdd;
        if (rdfSources.usesQuads()) {
          rdd = rdfSources.asQuads().toJavaRDD().flatMap(quad -> (QuadUtils.quadToList(quad).iterator()));
        } else {
          rdd = rdfSources.asTriples().toJavaRDD().flatMap(triple -> (TripleUtils.tripleToList(triple).iterator()));
        }

        Map<String, String> inputPm = model.getNsPrefixMap();
        Map<String, String> usedPm = JavaRddOps.aggregateUsingJavaCollector(rdd,
                NodeAnalytics.usedPrefixes(inputPm).asCollector());

        Model tmp = ModelFactory.createDefaultModel();
        tmp.setNsPrefixes(usedPm);
        RDFDataMgr.write(System.out, tmp, RDFFormat.TURTLE_PRETTY);
      }
    }.call();

    return 0;
  }
}

