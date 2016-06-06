package queryTranslator.op;

import org.apache.jena.atlas.lib.NotImplemented;

import queryTranslator.SqlOpVisitor;
import queryTranslator.Tags;
import queryTranslator.sql.SqlStatement;


import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;

/**
 * 
 * @author Alexander Schaetzle
 */
public class SqlReduced extends SqlOp1 {

	private final OpReduced opReduced;

	public SqlReduced(OpReduced _opReduced, SqlOp _subOp,
			PrefixMapping _prefixes) {
		super(_subOp, _prefixes);
		opReduced = _opReduced;
		resultName = Tags.REDUCED;
	}



	@Override
	public void visit(SqlOpVisitor sqlOpVisitor) {
		sqlOpVisitor.visit(this);
	}



	@Override
	public SqlStatement translate(String name, SqlStatement child) {
		// TODO Auto-generated method stub
		throw new NotImplemented();
	}

}
