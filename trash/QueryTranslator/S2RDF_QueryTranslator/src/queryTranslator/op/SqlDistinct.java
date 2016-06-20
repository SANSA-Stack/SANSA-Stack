package queryTranslator.op;

import queryTranslator.SqlOpVisitor;
import queryTranslator.Tags;
import queryTranslator.sql.SqlStatement;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;

/**
 * 
 * @author Antony Neu
 */
public class SqlDistinct extends SqlOp1 {

	private final OpDistinct opDistinct;

	public SqlDistinct(OpDistinct _opDistinct, SqlOp _subOp,
			PrefixMapping _prefixes) {
		super(_subOp, _prefixes);
		opDistinct = _opDistinct;
		resultName = Tags.DISTINCT;
	}

	@Override
	public void visit(SqlOpVisitor sqlOpVisitor) {
		sqlOpVisitor.visit(this);
	}

	@Override
	public SqlStatement translate(String name, SqlStatement child) {
		// TODO Auto-generated method stub
		return null;
	}

}
