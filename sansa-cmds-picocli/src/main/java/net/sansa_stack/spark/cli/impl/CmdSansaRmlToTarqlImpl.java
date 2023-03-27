package net.sansa_stack.spark.cli.impl;

import java.util.Collection;

import org.aksw.jenax.arq.util.syntax.QueryUtils;
import org.aksw.jenax.arq.util.update.UpdateUtils;
import org.aksw.r2rml.jena.arq.impl.R2rmlImporterLib;
import org.aksw.r2rml.jena.arq.impl.TriplesMapToSparqlMapping;
import org.apache.jena.query.Query;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;

import net.sansa_stack.spark.cli.cmd.CmdSansaRmlToTarql;

public class CmdSansaRmlToTarqlImpl {
    public static int run(CmdSansaRmlToTarql cmd) {
        for (String inputFile : cmd.inputFiles) {
            Model model = RDFDataMgr.loadModel(inputFile);

            UpdateUtils.renameProperty(model.getGraph(), "http://semweb.mmlab.be/ns/rml#reference", "http://www.w3.org/ns/r2rml#column");
            UpdateUtils.renameProperty(model.getGraph(), "http://semweb.mmlab.be/ns/rml#source", "http://www.w3.org/ns/r2rml#tableName");
            UpdateUtils.renameProperty(model.getGraph(), "http://semweb.mmlab.be/ns/rml#logicalSource", "http://www.w3.org/ns/r2rml#logicalTable");

            Collection<TriplesMapToSparqlMapping> maps = R2rmlImporterLib.read(model);

            // RDFDataMgr.write(System.out, model, RDFFormat.TURTLE_PRETTY);
            for (TriplesMapToSparqlMapping item : maps) {
                Query query = item.getAsQuery();
                QueryUtils.optimizePrefixes(query);
                System.out.println(item.getAsQuery());
            }
        }
        return 0;
    }
}
