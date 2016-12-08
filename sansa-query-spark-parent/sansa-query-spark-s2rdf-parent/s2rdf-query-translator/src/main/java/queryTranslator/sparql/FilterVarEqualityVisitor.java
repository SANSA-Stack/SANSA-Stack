package queryTranslator.sparql;

import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;
import java.util.List;

/**
 *
 * @author Alexander Schaetzle
 */
public class FilterVarEqualityVisitor extends OpVisitorBase {
    
    public List<Var> projectVars;

    public FilterVarEqualityVisitor() {}


    @Override
    public void visit(OpProject opProject) {
        projectVars = opProject.getVars();
    }

}
