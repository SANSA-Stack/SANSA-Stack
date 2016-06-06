package queryTranslator;


import queryTranslator.op.SqlBGP;
import queryTranslator.op.SqlDistinct;
import queryTranslator.op.SqlFilter;
import queryTranslator.op.SqlJoin;
import queryTranslator.op.SqlLeftJoin;
import queryTranslator.op.SqlOrder;
import queryTranslator.op.SqlProject;
import queryTranslator.op.SqlReduced;
import queryTranslator.op.SqlSequence;
import queryTranslator.op.SqlSlice;
import queryTranslator.op.SQLUnion;

/**
 *
 * @author Antony Neu
 */
public interface SqlOpVisitor {
    
    // Operators
    public void visit(SqlBGP sqlBGP);
    public void visit(SqlFilter sqlFilter);
    public void visit(SqlJoin sqlJoin);
    public void visit(SqlSequence sqlSequence);
    public void visit(SqlLeftJoin sqlLeftJoin);
    public void visit(SQLUnion sqlUnion);

    // Solution Modifier
    public void visit(SqlProject sqlProject);
    public void visit(SqlDistinct sqlDistinct);
    public void visit(SqlReduced sqlReduced);
    public void visit(SqlOrder sqlOrder);
    public void visit(SqlSlice sqlSlice);

}
