package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.spark.cli.cmd.CmdSansaPrefixesHead;
import net.sansa_stack.spark.cli.cmd.CmdSansaPrefixesUsed;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOps;
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

import java.io.IOException;
import java.util.Map;


/**
 * Called from the Java class {@link CmdSansaPrefixesHead}
 */
public class CmdSansaPrefixesHeadImpl {
  private static Logger logger = LoggerFactory.getLogger(CmdSansaPrefixesHeadImpl.class);

  public static int run(CmdSansaPrefixesHead cmd) throws IOException {

    new SimpleSparkCmdTemplate<>("Sansa Prefixes Used", cmd.inputConfig, cmd.inputFiles) {
      @Override
      protected void process() {
        Model model = rdfSources.peekPrefixes();
        RDFDataMgr.write(System.out, model, RDFFormat.TURTLE_PRETTY);
      }
    }.call();

    return 0;
  }
}

