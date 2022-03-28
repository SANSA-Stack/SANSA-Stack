package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.spark.cli.cmd.CmdSansaPrefixesHead;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * Called from the Java class {@link CmdSansaPrefixesHead}
 */
public class CmdSansaPrefixesHeadImpl {
  private static Logger logger = LoggerFactory.getLogger(CmdSansaPrefixesHeadImpl.class);

  public static int run(CmdSansaPrefixesHead cmd) throws IOException {

    new SimpleSparkCmdTemplate<>("Sansa Prefixes Used", cmd.inputConfig, cmd.inputFiles) {
      @Override
      protected void process() {
        Model model = rdfSources.peekDeclaredPrefixes();
        RDFDataMgr.write(System.out, model, RDFFormat.TURTLE_PRETTY);
      }
    }.call();

    return 0;
  }
}

