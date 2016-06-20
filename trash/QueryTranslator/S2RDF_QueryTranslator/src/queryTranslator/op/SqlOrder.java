package queryTranslator.op;

import java.util.Iterator;
import java.util.List;

import queryTranslator.SqlOpVisitor;
import queryTranslator.Tags;
import queryTranslator.sparql.ExprTranslator;
import queryTranslator.sql.SqlStatement;


import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.SortCondition;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.expr.E_Function;
import com.hp.hpl.jena.sparql.expr.Expr;

/**
 * 
 * @author Alexander Schaetzle
 */
public class SqlOrder extends SqlOp1 {

	private final OpOrder opOrder;

	public SqlOrder(OpOrder _opOrder, SqlOp _subOp,
			PrefixMapping _prefixes) {
		super(_subOp, _prefixes);
		opOrder = _opOrder;

	}

	public SqlStatement translate(String _resultName, SqlStatement child) {
		resultName = subOp.getResultName();
		String order = "";
		resultSchema = subOp.getSchema();

		List<SortCondition> conditions = opOrder.getConditions();
		Iterator<SortCondition> iterator = conditions.iterator();
		SortCondition current = iterator.next();
		order += getOrderArg(current);
		while (iterator.hasNext()) {
			current = iterator.next();
			order += ", " + getOrderArg(current);
		}
		child.addOrder(order);
		child.addLimit(Tags.LIMIT_LARGE_NUMBER);
		return child;
	}

	private String getOrderArg(SortCondition condition) {
		Expr expr = condition.getExpression();
		String orderArg ="";
		if(expr instanceof E_Function){
			
			ExprTranslator translator = new ExprTranslator(prefixes);
			orderArg = translator.translate(expr,
					expandPrefixes,resultSchema);
		}else{
			orderArg = expr.getVarName();
		}
		int direction = condition.getDirection();

		switch (direction) {
		case Query.ORDER_ASCENDING: {
			orderArg += " ASC";
			break;
		}
		case Query.ORDER_DESCENDING: {
			orderArg += " DESC";
			break;
		}
		case Query.ORDER_DEFAULT: {
			break;
		}
		}

		return orderArg;
	}

	@Override
	public void visit(SqlOpVisitor sqlOpVisitor) {
		sqlOpVisitor.visit(this);
	}

}
