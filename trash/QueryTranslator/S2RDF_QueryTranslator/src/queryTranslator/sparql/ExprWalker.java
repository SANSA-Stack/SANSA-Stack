package queryTranslator.sparql;

import com.hp.hpl.jena.sparql.expr.*;

/**
 *
 * @author Alexander Schaetzle
 */
public class ExprWalker extends ExprVisitorFunction {

    private final ExprVisitor visitor;
    private final boolean topDown;


    private ExprWalker(ExprVisitor visitor, boolean topDown) {
        this.visitor = visitor;
        this.topDown = topDown;
    }

    public static void walkTopDown(ExprVisitor visitor, Expr expr) {
        expr.visit(new ExprWalker(visitor, true));
    }

    public static void walkBottomUp(ExprVisitor visitor, Expr expr) {
        expr.visit(new ExprWalker(visitor, false));
    }


    
    @Override
    public void startVisit() {}

    @Override
    public void visitExprFunction(ExprFunction func) {
        if ( topDown ) {
            func.visit(visitor) ;
        }
        for ( int i = 1 ; i <= func.numArgs() ; i++ )
        {
            Expr expr = func.getArg(i) ;
            if ( expr == null ) {
                break ;
            }
            expr.visit(this) ;
        }
        if ( !topDown ) {
            func.visit(visitor) ;
        }
    }

    @Override
    public void visit(ExprFunctionOp funcOp) {
        funcOp.visit(visitor) ;
    }

    @Override
    public void visit(NodeValue nv) {
        nv.visit(visitor) ;
    }

    @Override
    public void visit(ExprVar nv) {
        nv.visit(visitor) ;
    }

    @Override
    public void visit(ExprAggregator eAgg) {
        eAgg.visit(visitor) ;
    }
    
    @Override
    public void finishVisit() { }

}
