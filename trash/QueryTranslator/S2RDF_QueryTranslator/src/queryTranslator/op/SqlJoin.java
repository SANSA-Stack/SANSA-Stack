package queryTranslator.op;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import queryTranslator.SqlOpVisitor;
import queryTranslator.Tags;
import queryTranslator.sparql.ExprTranslator;
import queryTranslator.sql.SqlStatement;
import queryTranslator.sql.Join;
import queryTranslator.sql.JoinType;
import queryTranslator.sql.JoinUtil;
import queryTranslator.sql.Schema;


import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.expr.Expr;

/**
 * 
 * @author Alexander Schaetzle
 */
public class SqlJoin extends SqlOp2 {

	private final OpJoin opJoin;

	public SqlJoin(OpJoin _opJoin, SqlOp _leftOp, SqlOp _rightOp,
			PrefixMapping _prefixes) {
		super(_leftOp, _rightOp, _prefixes);
		opJoin = _opJoin;
		resultName = Tags.JOIN;
	}

	@Override
	public SqlStatement translate(String _resultName, SqlStatement firstChild,
			SqlStatement secondChild) {
		
		resultName = _resultName;
		
		Map<String, String[]> newSchema = new HashMap<String, String[]>();
        newSchema.putAll(leftOp.getSchema());
        newSchema.putAll(rightOp.getSchema());
		resultSchema = Schema.shiftToParent(newSchema, this.resultName);
		
		
		SqlStatement join = null ;
		
		List<SqlStatement> rights = new ArrayList<SqlStatement>();
		rights.add(secondChild);
				
		List<String> onConditions =  JoinUtil.getOnConditions(Schema.shiftToParent(leftOp.getSchema(), leftOp.getResultName()), Schema.shiftToParent(rightOp.getSchema(), rightOp.getResultName()));
		
		
		join = new Join(this.getResultName(), firstChild, rights, onConditions, JoinType.natural);
		
		
		
		 return join;

	}

	

	@Override
	public void visit(SqlOpVisitor sqlOpVisitor) {
		sqlOpVisitor.visit(this);
	}

}
