package queryTranslator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.jena.atlas.lib.NotImplemented;

import queryTranslator.op.SqlBGP;
import queryTranslator.op.SqlDistinct;
import queryTranslator.op.SqlFilter;
import queryTranslator.op.SqlJoin;
import queryTranslator.op.SqlLeftJoin;
import queryTranslator.op.SqlOp;
import queryTranslator.op.SqlOrder;
import queryTranslator.op.SqlProject;
import queryTranslator.op.SqlReduced;
import queryTranslator.op.SqlSequence;
import queryTranslator.op.SqlSlice;
import queryTranslator.op.SQLUnion;
import queryTranslator.sql.SqlStatement;
import queryTranslator.sql.Join;
import queryTranslator.sql.JoinType;
import queryTranslator.sql.JoinUtil;
import queryTranslator.sql.Schema;


/**
 * Translates a SqlOp Tree into a corresponding SqlQL program. It walks
 * through the SqlOp Tree bottom up and generates the commands for every
 * operator. A SPARQL Algebra Tree must be translated into a corresponding SqlOp
 * Tree before using the SqlOpTranslator.
 * 
 * @see queryTranslator.sparql.AlgebraTransformer
 */
public class SqlOpTranslator extends SqlOpVisitorBase {

	// Expand prefixes or not
	private boolean expandPrefixes;
	// Count the occurences of an operator
	private int countBGP,countPropertyPathBGP, countJoin, countLeftJoin, countUnion, countSequence,
			countFilter;

	private Stack<SqlStatement> stack = new Stack<SqlStatement>();

	/**
	 * Constructor of class SqlOpTranslator.
	 */
	public SqlOpTranslator() {
		countBGP = 0;
		countPropertyPathBGP = 0;
		countJoin = 0;
		countLeftJoin = 0;
		countUnion = 0;
		countSequence = 0;
		countFilter = 0;
	}

	/**
	 * Translates a SqlOp Tree into a corresponding SqlQL program.
	 * 
	 * @param op
	 *            Root of the SqlOp Tree
	 * @param _expandPrefixes
	 *            Expand prefixes used in the original query or not
	 * @return SqlQL program
	 */
	public String translate(SqlOp op, boolean _expandPrefixes) {
		expandPrefixes = _expandPrefixes;
		// Walk through the tree bottom up
		SqlOpWalker.walkBottomUp(this, op);
		// put stack top here
		String result = stack.pop().toString();
		int left = result.indexOf("(");
		int right = result.lastIndexOf(")");
		// (simon) remove outer select
		left = result.indexOf("(", left + 1);
		right = result.lastIndexOf(")",right - 1);
		result = result.substring(left+1,right);
		String raw = Tags.QUERY_PREFIX + result  ;
		return clean(raw);
	}

	private String clean(String raw) {
		StringBuilder sb = new StringBuilder();
		Stack<Character> chars = new Stack<Character>();
		for (Character c : raw.toCharArray()) {
			if (c.equals('\'')) {
				if (!chars.isEmpty() && chars.peek().equals('\'')) {
					chars.pop();
				} else {
					chars.push('\'');
				}
			} else if (!chars.isEmpty() && c.equals('"')) {
				if (chars.peek().equals('"')) {
					chars.pop();
				} else {
					chars.push('"');
				}
			}
			if (c.equals(':')) {
				if (chars.isEmpty()) {
					sb.append('_');
				} else {
					sb.append(':');
				}
			} else {
				sb.append(c);
			}

		}
		return sb.toString();
	}

	/**
	 * Translates a BGP into corresponding SqlQL commands.
	 * 
	 * @param bgp
	 *            BGP in the SqlOp Tree
	 */
	@Override
	public void visit(SqlBGP bgp) {
		countBGP++;
		bgp.setExpandMode(expandPrefixes);
		stack.push(bgp.translate(Tags.BGP + countBGP));
	}

	/**
	 * Translates a FILTER into corresponding SqlQL commands.
	 * 
	 * @param filter
	 *            FILTER in the SqlOp Tree
	 */
	@Override
	public void visit(SqlFilter filter) {
		countFilter++;
		stack.push(filter.translate(Tags.FILTER + countFilter, stack.pop()));
	}

	/**
	 * Translates a JOIN into corresponding SQL commands.
	 * 
	 * @param join
	 *            JOIN in the SqlOp Tree
	 */
	@Override
	public void visit(SqlJoin join) {
		countJoin++;
		SqlStatement right = stack.pop();
		SqlStatement left = stack.pop();
		stack.push(join.translate(Tags.JOIN + countJoin, left, right));
	}

	/**
	 * Translates a sequence of JOINs into corresponding SqlQL Latin commands.
	 * 
	 * @param sequence
	 *            JOIN sequence in the SqlOp Tree
	 */
	@Override
	public void visit(SqlSequence sequence) {
		Stack<SqlStatement> children_rev = new Stack<SqlStatement>();
		for (int i = 0; i < sequence.size(); i++) {
			children_rev.push(stack.pop());
		}
		List<SqlStatement> children = new ArrayList<SqlStatement>();
		for (int i = 0; i < sequence.size(); i++) {
			children.add(children_rev.pop());
		}

		countJoin++;
		sequence.setResultName(Tags.SEQUENCE + countJoin);

		Map<String, String[]> filterSchema = new HashMap<String, String[]>();
		String tablename = sequence.getResultName();
		List<String> onStrings = new ArrayList<String>();
		for (int i = 1; i < children.size(); i++) {
			Map<String, String[]> leftschema = Schema.shiftToParent(sequence
					.getElements().get(i - 1).getSchema(), sequence
					.getElements().get(i - 1).getResultName());
			Map<String, String[]> rightschema = Schema.shiftToParent(sequence
					.getElements().get(i).getSchema(), sequence.getElements()
					.get(i).getResultName());
			filterSchema.putAll(leftschema);
			onStrings.add(JoinUtil.generateConjunction(JoinUtil
					.getOnConditions(filterSchema, rightschema)));
			filterSchema.putAll(rightschema);
		}

		sequence.setSchema(filterSchema);
		Join join = new Join(tablename, children.get(0), children.subList(1,
				children.size()), onStrings, JoinType.natural);

		stack.push(join);
	}

	/**
	 * Translates a LEFTJOIN into corresponding SqlQL commands.
	 * 
	 * @param SqlLeftJoin
	 *            LEFTJOIN in the SqlOp Tree
	 */
	@Override
	public void visit(SqlLeftJoin sqlLeftJoin) {

		SqlStatement right = stack.pop();
		SqlStatement left = stack.pop();
		stack.push(sqlLeftJoin.translate(Tags.LEFT_JOIN + countLeftJoin++,
				left, right));
	}


	/**
	 * Translates a UNION into corresponding SqlQL commands.
	 * 
	 * @param SQLUnion
	 *            UNION in the SqlOp Tree
	 */
	@Override
	public void visit(SQLUnion union) {
		countUnion++;
		SqlStatement right = stack.pop();
		SqlStatement left = stack.pop();
		SqlStatement s = union.translate(Tags.UNION + countUnion, left, right);
		stack.push(s);
	}

	/**
	 * Translates a PROJECT into corresponding SqlQL commands.
	 * 
	 * @param SqlProject
	 *            PROJECT in the SqlOp Tree
	 */
	@Override
	public void visit(SqlProject sqlProject) {
		SqlStatement projection = sqlProject.translate(Tags.PROJECT,
				stack.pop());
		stack.push(projection);
	}

	/**
	 * Translates a DISTINCT into corresponding SqlQL commands.
	 * 
	 * @param SqlDistinct
	 *            Distinct in the SqlOp Tree
	 */
	@Override
	public void visit(SqlDistinct distinct) {
		SqlStatement sql = stack.pop();
		sql.setDistinct(true);
		stack.push(sql);
	}

	/**
	 * Translates a REDUCE into corresponding SqlQL commands.
	 * 
	 * @param SqlReduced
	 *            REDUCE in the SqlOp Tree
	 */
	@Override
	public void visit(SqlReduced sqlReduced) {
		throw new NotImplemented();
	}

	/**
	 * Translates an ORDER into corresponding SQL commands.
	 * 
	 * @param SqlOrder
	 *            ORDER in the SqlOp Tree
	 */
	@Override
	public void visit(SqlOrder sqlOrder) {
		SqlStatement s = sqlOrder.translate(Tags.ORDER, stack.pop());
		stack.push(s);

	}

	/**
	 * Translates a SLICE into corresponding SqlQL commands.
	 * 
	 * @param SqlSlice
	 *            SLICE in the SqlOp Tree
	 */
	@Override
	public void visit(SqlSlice slice) {
		SqlStatement child = stack.pop();
		stack.push(slice.translate(child.getName(), child));
	}

}
