package queryTranslator.op;

import java.util.Iterator;

import javax.xml.validation.Schema;

import queryTranslator.SqlOpVisitor;
import queryTranslator.Tags;
import queryTranslator.sparql.ExprTranslator;
import queryTranslator.sql.SqlStatement;


import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.expr.Expr;

/**
 * 
 * @author Antony Neu
 */
public class SqlFilter extends SqlOp1 {

	private final OpFilter opFilter;

	public SqlFilter(OpFilter _opFilter, SqlOp _subOp,
			PrefixMapping _prefixes) {
		super(_subOp, _prefixes);
		opFilter = _opFilter;
		resultName = Tags.FILTER;
	}

	public SqlStatement translate(String _resultName, SqlStatement child) {
		resultName = subOp.getResultName();
		this.resultSchema = subOp.getSchema();
		Iterator<Expr> iterator = opFilter.getExprs().iterator();
		Expr current = iterator.next();
		ExprTranslator translator = new ExprTranslator(prefixes);
		String condition = translator.translate(current,
				expandPrefixes,resultSchema);
		if(!condition.equals("")){
			child.addConjunction(condition); 
		}
		while (iterator.hasNext()) {
			translator = new ExprTranslator(prefixes);
			current = iterator.next();
			condition = translator.translate(current,
					expandPrefixes,resultSchema);
			//child.updateSelection(resultSchema); // consider schema changes 
			if(!condition.equals("")){
				child.addConjunction(condition);
			}
		}
		
		

		return child;
	}



	@Override
	public void visit(SqlOpVisitor sqlOpVisitor) {
		sqlOpVisitor.visit(this);
	}

}
