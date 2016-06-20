package queryTranslator.op;

import queryTranslator.SqlOpVisitor;
import queryTranslator.Tags;
import queryTranslator.sql.SqlStatement;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;

/**
 * 
 * @author Alexander Schaetzle
 */
public class SqlSlice extends SqlOp1 {

	private final OpSlice opSlice;

	public SqlSlice(OpSlice _opSlice, SqlOp _subOp,
			PrefixMapping _prefixes) {
		super(_subOp, _prefixes);
		opSlice = _opSlice;
		resultName = Tags.SLICE;
	}


	@Override
	public SqlStatement translate(String _resultName, SqlStatement child) {
		resultName = subOp.getResultName();
		resultSchema = subOp.getSchema();
		
        if(opSlice.getStart() > 0) {
		child.addOffset((int)  opSlice.getStart());
        }
        if(opSlice.getLength() > 0) {
        	child.addLimit((int) opSlice.getLength());
        }

		return child;
	}

	@Override
	public void visit(SqlOpVisitor sqlOpVisitor) {
		sqlOpVisitor.visit(this);
	}

}
