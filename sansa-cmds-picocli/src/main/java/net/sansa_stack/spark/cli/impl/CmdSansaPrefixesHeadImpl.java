package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.spark.cli.cmd.CmdSansaPrefixesHead;
import net.sansa_stack.spark.cli.util.SimpleSparkCmdRdfTemplate;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.PrefixMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Called from the Java class {@link CmdSansaPrefixesHead}
 */
public class CmdSansaPrefixesHeadImpl {
  private static Logger logger = LoggerFactory.getLogger(CmdSansaPrefixesHeadImpl.class);

  public static int run(CmdSansaPrefixesHead cmd) throws Exception {

    new SimpleSparkCmdRdfTemplate<>("Sansa Prefixes Used", cmd.inputConfig, cmd.inputFiles) {
      @Override
      protected void process() throws Exception {
        PrefixMap prefixMap = rdfSources.peekDeclaredPrefixes();
        Model model = ModelFactory.createDefaultModel();
        model.setNsPrefixes(prefixMap.getMapping());
        RDFDataMgr.write(System.out, model, RDFFormat.TURTLE_PRETTY);
      }
    }.call();

    return 0;
  }
}

