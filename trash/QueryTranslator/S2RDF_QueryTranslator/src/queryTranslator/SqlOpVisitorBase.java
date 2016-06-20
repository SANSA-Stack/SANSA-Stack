package queryTranslator;


import org.apache.log4j.Logger;

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
public class SqlOpVisitorBase implements SqlOpVisitor {
    
    // Define a static logger variable so that it references the corresponding Logger instance
    protected static Logger logger = Logger.getLogger(SqlOpVisitor.class);

    // OPERATORS
    @Override
    public void visit(SqlBGP sqlBGP) {
        logger.error("BGP not supported yet!");
        throw new UnsupportedOperationException("BGP not supported yet!");
    }

    @Override
    public void visit(SqlFilter sqlFilter) {
        logger.error("FILTER not supported yet!");
        throw new UnsupportedOperationException("FILTER not supported yet!");
    }

    @Override
    public void visit(SqlJoin sqlJoin) {
        logger.error("JOIN not supported yet!");
        throw new UnsupportedOperationException("JOIN not supported yet!");
    }

    @Override
    public void visit(SqlSequence sqlSequence) {
        logger.error("SEQUENCE not supported yet!");
        throw new UnsupportedOperationException("SEQUENCE not supported yet!");
    }

    @Override
    public void visit(SqlLeftJoin sqlLeftJoin) {
        logger.error("LEFTJOIN not supported yet!");
        throw new UnsupportedOperationException("LEFTJOIN not supported yet!");
    }

    @Override
    public void visit(SQLUnion sqlUnion) {
        logger.error("UNION not supported yet!");
        throw new UnsupportedOperationException("UNION not supported yet!");
    }

    // SOLUTION MODIFIERS
    @Override
    public void visit(SqlProject sqlProject) {
        logger.error("PROJECT not supported yet!");
        throw new UnsupportedOperationException("PROJECT not supported yet!");
    }

    @Override
    public void visit(SqlDistinct sqlDistinct) {
        logger.error("DISTINCT not supported yet!");
        throw new UnsupportedOperationException("DISTINCT not supported yet!");
    }

    @Override
    public void visit(SqlReduced sqlReduced) {
        logger.error("REDUCED not supported yet!");
        throw new UnsupportedOperationException("REDUCED not supported yet!");
    }

    @Override
    public void visit(SqlOrder sqlOrder) {
        logger.error("ORDER not supported yet!");
        throw new UnsupportedOperationException("ORDER not supported yet!");
    }

    @Override
    public void visit(SqlSlice sqlSlice) {
        logger.error("SLICE not supported yet!");
        throw new UnsupportedOperationException("SLICE not supported yet!");
    }
    
    

}
