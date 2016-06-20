package queryTranslator;

import java.util.Iterator;

import queryTranslator.op.SqlOp;
import queryTranslator.op.SqlOp1;
import queryTranslator.op.SqlOp2;
import queryTranslator.op.SqlOpN;


/**
 * Applies a given sqlOpVisitor to all operators in the tree Can walk through
 * the tree bottom up or top down.
 * 
 * @author Alexander Schaetzle
 * @see SqlOpVisitor.sql.sqlOpVisitor
 */
public class SqlOpWalker extends SqlOpVisitorByType {

	// Visitor to be applied to all operators in the tree below the given
	// operator
	private final SqlOpVisitor visitor;
	// Walk through tree bottom up or top down
	private final boolean topDown;

	/**
	 * Private constructor, initialization using factory functions.
	 * 
	 * @param visitor
	 *            sqlOpVisitor to be applied
	 * @param topDown
	 *            true - top down, false - bottom up
	 * @see #walkBottomUp(SqlOpVisitor.sql.sqlOpVisitor,
	 *      SqlOp.sql.op.sqlOp)
	 * @see #walkTopDown(SqlOpVisitor.sql.sqlOpVisitor,
	 *      SqlOp.sql.op.sqlOp)
	 */
	private SqlOpWalker(SqlOpVisitor visitor, boolean topDown) {
		this.visitor = visitor;
		this.topDown = topDown;
	}

	/**
	 * Apply a given sqlOpVisitor to all operators in a sqlOp tree walking top
	 * down.
	 * 
	 * @param visitor
	 *            sqlOpVisitor to be applied
	 * @param op
	 *            Root of sqlOp tree
	 * @see SqlOpVisitor.sql.sqlOpVisitor
	 */
	public static void walkTopDown(SqlOpVisitor visitor, SqlOp op) {
		op.visit(new SqlOpWalker(visitor, true));
	}

	/**
	 * Apply a given sqlOpVisitor to all operators in a sqlOp tree walking
	 * bottom up.
	 * 
	 * @param visitor
	 *            sqlOpVisitor to be applied
	 * @param op
	 *            Root of sqlOp tree
	 * @see SqlOpVisitor.sql.sqlOpVisitor
	 */
	public static void walkBottomUp(SqlOpVisitor visitor, SqlOp op) {
		op.visit(new SqlOpWalker(visitor, false));
	}

	/**
	 * Visit leef operator with no sub operators.
	 * 
	 * @param op
	 */
	@Override
	protected void visit0(SqlOp op) {
		op.visit(visitor);
	}

	/**
	 * Visit operator with 1 sub operator.
	 * 
	 * @param op
	 */
	@Override
	protected void visit1(SqlOp1 op) {
		if (topDown)
			op.visit(visitor);
		if (op.getSubOp() != null)
			op.getSubOp().visit(this);
		else
			logger.warn("Sub operator is missing in " + op.getResultName());
		if (!topDown)
			op.visit(visitor);
	}

	/**
	 * Visit operator with 2 sub operator.
	 * 
	 * @param op
	 */
	@Override
	protected void visit2(SqlOp2 op) {
		if (topDown)
			op.visit(visitor);
		if (op.getLeft() != null)
			op.getLeft().visit(this);
		else
			logger.warn("Left sub operator is missing in " + op.getResultName());
		if (op.getRight() != null)
			op.getRight().visit(this);
		else
			logger.warn("Right sub operator is missing in " + op.getResultName());
		if (!topDown)
			op.visit(visitor);
	}

	/**
	 * Visit operator with N sub operator.
	 * 
	 * @param op
	 */
	@Override
	protected void visitN(SqlOpN op) {
		if (topDown)
			op.visit(visitor);
		for (Iterator<SqlOp> iter = op.iterator(); iter.hasNext();) {
			SqlOp sub = iter.next();
			sub.visit(this);
		}
		if (!topDown)
			op.visit(visitor);
	}

}
