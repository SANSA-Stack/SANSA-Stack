package queryTranslator;

import java.io.PrintWriter;
import java.util.Iterator;

import queryTranslator.op.SqlOp;
import queryTranslator.op.SqlOp1;
import queryTranslator.op.SqlOp2;
import queryTranslator.op.SqlOpN;


/**
 * Class for printing a SqlOp tree.
 */
public class SqlOpPrettyPrinter extends SqlOpVisitorByType {

	private final PrintWriter writer;
	private String offset;

	/**
	 * Private constructor, initialization using factory function.
	 * 
	 * @param _writer
	 */
	private SqlOpPrettyPrinter(PrintWriter _writer) {
		offset = "";
		writer = _writer;
	}

	/**
	 * Prints the given SqlOp tree.
	 * 
	 * @param _writer
	 * @param op
	 */
	public static void print(PrintWriter _writer, SqlOp op) {
		op.visit(new SqlOpPrettyPrinter(_writer));
	}

	@Override
	protected void visit0(SqlOp op) {
		writer.println(offset + op.getResultName());
	}

	@Override
	protected void visit1(SqlOp1 op) {
		writer.println(offset + op.getResultName() + "(");
		offset += "  ";
		if (op.getSubOp() != null)
			op.getSubOp().visit(this);
		offset = offset.substring(2);
		writer.println(")");
	}

	@Override
	protected void visit2(SqlOp2 op) {
		writer.println(offset + op.getResultName() + "(");
		offset += "  ";
		if (op.getLeft() != null)
			op.getLeft().visit(this);
		if (op.getRight() != null)
			op.getRight().visit(this);
		offset = offset.substring(2);
		writer.println(")");
	}

	@Override
	protected void visitN(SqlOpN op) {
		writer.println(offset + op.getResultName() + "(");
		offset += "  ";
		for (Iterator<SqlOp> iter = op.iterator(); iter.hasNext();) {
			SqlOp sub = iter.next();
			sub.visit(this);
		}
		offset = offset.substring(2);
		writer.println(")");
	}

}
