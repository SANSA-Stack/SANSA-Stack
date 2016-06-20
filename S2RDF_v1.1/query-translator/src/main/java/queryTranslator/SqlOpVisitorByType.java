package queryTranslator;


import org.apache.log4j.Logger;

import queryTranslator.op.SqlBGP;
import queryTranslator.op.SqlDistinct;
import queryTranslator.op.SqlFilter;
import queryTranslator.op.SqlJoin;
import queryTranslator.op.SqlLeftJoin;
import queryTranslator.op.SqlOp;
import queryTranslator.op.SqlOp1;
import queryTranslator.op.SqlOp2;
import queryTranslator.op.SqlOpN;
import queryTranslator.op.SqlOrder;
import queryTranslator.op.SqlProject;
import queryTranslator.op.SqlReduced;
import queryTranslator.op.SqlSequence;
import queryTranslator.op.SqlSlice;
import queryTranslator.op.SQLUnion;


/**
 *
 * @author Alexander Schaetzle
 */
public abstract class SqlOpVisitorByType implements SqlOpVisitor {
    
    // Define a static logger variable so that it references the corresponding Logger instance
    protected static Logger logger = Logger.getLogger(SqlOpVisitor.class);
    

    /**
     * Operators with no sub operators
     * @param op 
     */
    protected abstract void visit0(SqlOp op);

    /**
     * Operators with 1 sub operator
     * @param op 
     */ 
    protected abstract void visit1(SqlOp1 op);

    /**
     * Operators with 2 sub operators
     * @param op 
     */
    protected abstract void visit2(SqlOp2 op);

    /**
     * Operators with N sub operators
     * @param op 
     */
    protected abstract void visitN(SqlOpN op);
    

    // Declare basic visit methods as final such that derived classes cannot override it
    // OPERATORS
    @Override
    public final void visit(SqlBGP sqlBGP) {
        visit0(sqlBGP);
    }
    
    @Override
    public final void visit(SqlFilter sqlFilter) {
        visit1(sqlFilter);
    }

    @Override
    public final void visit(SqlJoin sqlJoin) {
        visit2(sqlJoin);
    }

    @Override
    public final void visit(SqlSequence sqlSequence) {
        visitN(sqlSequence);
    }

    @Override
    public final void visit(SqlLeftJoin sqlLeftJoin) {
        visit2(sqlLeftJoin);
    }

    @Override
    public final void visit(SQLUnion sqlUnion) {
        visit2(sqlUnion);
    }

    // SOLUTION MODIFIERS
    @Override
    public final void visit(SqlProject sqlProject) {
        visit1(sqlProject);
    }

    @Override
    public final void visit(SqlDistinct sqlDistinct) {
        visit1(sqlDistinct);
    }

    @Override
    public final void visit(SqlReduced sqlReduced) {
        visit1(sqlReduced);
    }

    @Override
    public final void visit(SqlOrder sqlOrder) {
        visit1(sqlOrder);
    }

    @Override
    public final void visit(SqlSlice sqlSlice) {
        visit1(sqlSlice);
    }

}
