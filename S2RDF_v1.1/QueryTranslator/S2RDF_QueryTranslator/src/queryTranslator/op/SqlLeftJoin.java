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
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.expr.Expr;

/**
 * 
 * @author Alexander Schaetzle
 */
public class SqlLeftJoin extends SqlOp2 {

	private final OpLeftJoin opLeftJoin;

	public SqlLeftJoin(OpLeftJoin _opLeftJoin, SqlOp _leftOp,
			SqlOp _rightOp, PrefixMapping _prefixes) {
		super(_leftOp, _rightOp, _prefixes);
		opLeftJoin = _opLeftJoin;
	}

	@Override
	public SqlStatement translate(String _resultName, SqlStatement firstChild,
			SqlStatement secondChild) {

		resultName = _resultName;
		SqlStatement leftjoin = null;

		Map<String, String[]> newSchema = new HashMap<String, String[]>();
		newSchema.putAll(leftOp.getSchema());
		newSchema.putAll(rightOp.getSchema());

		Map<String, String[]> filterSchema = new HashMap<String, String[]>();
		filterSchema.putAll(Schema.shiftToParent(leftOp.getSchema(),
				leftOp.getResultName()));
		filterSchema.putAll(Schema.shiftToParent(rightOp.getSchema(),
				rightOp.getResultName()));

		resultSchema = filterSchema;

		String filter = "";
		// FILTER within OPTIONAL
		if (opLeftJoin.getExprs() != null) {
			Iterator<Expr> iterator = opLeftJoin.getExprs().iterator();
			Expr current = iterator.next();
			ExprTranslator translator = new ExprTranslator(prefixes);

			filter = translator
					.translate(current, expandPrefixes, filterSchema);
			while (iterator.hasNext()) {
				current = iterator.next();

				filter += " AND "
						+ translator.translate(current, expandPrefixes,
								filterSchema);
			}
		}
		List<SqlStatement> rights = new ArrayList<SqlStatement>();
		secondChild.removeNullFilters();
		rights.add(secondChild);

		List<String> onConditions = JoinUtil.getOnConditions(Schema
				.shiftToParent(leftOp.getSchema(), leftOp.getResultName()),
				Schema.shiftToParent(rightOp.getSchema(),
						rightOp.getResultName()));
		if (opLeftJoin.getExprs() != null) {
			onConditions.add(filter);
		}
		List<String> oneCondition = new ArrayList<String>();
		oneCondition.add(JoinUtil.generateConjunction(onConditions));

		if (JoinUtil
				.getSharedVars(
						Schema.shiftToParent(leftOp.getSchema(),
								leftOp.getResultName()),
						Schema.shiftToParent(rightOp.getSchema(),
								rightOp.getResultName())).size() > 0) {
			leftjoin = new Join(this.getResultName(), firstChild, rights,
					oneCondition, JoinType.left_outer);
		} else {
			leftjoin = new Join(this.getResultName(), firstChild, rights,
					oneCondition, JoinType.cross);
		}

		return leftjoin;
	}

	@Override
	public void visit(SqlOpVisitor sqlOpVisitor) {
		sqlOpVisitor.visit(this);
	}

}
