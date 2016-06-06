package queryTranslator.op;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import queryTranslator.SqlOpVisitor;
import queryTranslator.Tags;
import queryTranslator.sql.SqlStatement;
import queryTranslator.sql.Schema;
import queryTranslator.sql.Select;
import queryTranslator.sql.Union;


import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;

/**
 * 
 * @author Antony Neu
 */
public class SQLUnion extends SqlOp2 {

	private final OpUnion opUnion;

	public SQLUnion(OpUnion _opUnion, SqlOp _leftOp, SqlOp _rightOp,
			PrefixMapping _prefixes) {
		super(_leftOp, _rightOp, _prefixes);
		opUnion = _opUnion;
		resultName = Tags.UNION;
	}

	public SqlStatement translate(String _resultName, SqlStatement left, SqlStatement right) {
		resultName = _resultName;
		HashMap<String, String[]> selectionLeft = left.getSelectors();
		left.setMappings(selectionLeft);
		HashMap<String, String[]> selectionRight = right.getSelectors();
		right.setMappings(selectionRight);
		
		// make common schema
		ArrayList<String> onlyLeftVars = ((SqlBase) leftOp).getVarsOnlnyInOp((SqlBase)rightOp);
		for(String col : onlyLeftVars){
			right.addSelector(col, new String[]{ "null"});
		}
		ArrayList<String> onlyRightVars = ((SqlBase) rightOp).getVarsOnlnyInOp((SqlBase)leftOp);
		for(String col : onlyRightVars){
			left.addSelector(col, new String[]{ "null"});
		}
		// union schemas
		resultSchema.putAll(leftOp.getSchema());
		resultSchema.putAll(rightOp.getSchema());
		return new Union(getResultName(), left, right );
	}

	@Override
	public void visit(SqlOpVisitor sqlOpVisitor) {
		sqlOpVisitor.visit(this);
	}

}
