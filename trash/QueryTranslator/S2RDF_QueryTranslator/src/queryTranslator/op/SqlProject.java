package queryTranslator.op;

import queryTranslator.SqlOpVisitor;
import queryTranslator.Tags;
import queryTranslator.sql.SqlStatement;
import queryTranslator.sql.Schema;
import queryTranslator.sql.Select;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;

/**
 * 
 * @author Antony Neu
 */
public class SqlProject extends SqlOp1 {

	private final OpProject opProject;

	public SqlProject(OpProject _opProject, SqlOp _subOp,
			PrefixMapping _prefixes) {
		super(_subOp, _prefixes);
		opProject = _opProject;
		resultName = Tags.PROJECT;
	}


	@Override
	public SqlStatement translate(String _resultName, SqlStatement child) {
		// update schema
		resultSchema = Schema.shiftToParent(subOp.getSchema(), subOp.getResultName());
		resultName = _resultName;

		// translates to select object
		Select projection = new Select(this.getResultName());
		
		// Add selectors (subset of result schema)
		boolean first = true;
		for (Var var : this.opProject.getVars()) {
			projection.addSelector(var.getName(), resultSchema.get(var.getName()));
			first = false;
		}
		
		
		// set from
		projection.setFrom(child.toNamedString());
		
		return projection;
	}

	public OpProject getOpProject(){
		return this.opProject;
	}
	
	@Override
	public void visit(SqlOpVisitor sqlOpVisitor) {
		sqlOpVisitor.visit(this);
	}
	
	

}
