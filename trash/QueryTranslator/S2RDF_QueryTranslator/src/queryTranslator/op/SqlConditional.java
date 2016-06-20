package queryTranslator.op;

import queryTranslator.SqlOpVisitor;
import queryTranslator.sql.SqlStatement;

import com.hp.hpl.jena.shared.PrefixMapping;


/**
 *
 * @author Alexander Schaetzle
 */



public class SqlConditional extends SqlOp2 {

	protected SqlConditional(SqlOp _leftOp, SqlOp _rightOp,
			PrefixMapping _prefixes) {
		super(_leftOp, _rightOp, _prefixes);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void visit(SqlOpVisitor sqlOpVisitor) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public SqlStatement translate(String name, SqlStatement left,
			SqlStatement right) {
		// TODO Auto-generated method stub
		return null;
	}

    
    
}


