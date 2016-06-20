package queryTranslator.sparql;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVars;
import com.hp.hpl.jena.sparql.algebra.TransformCopy;
import com.hp.hpl.jena.sparql.algebra.Transformer;
import com.hp.hpl.jena.sparql.algebra.op.*;
import com.hp.hpl.jena.sparql.core.Substitute;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprList;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import java.util.Set;

/**
 *
 * @author Alexander Schaetzle
 */
public class TransformFilterVarEquality extends TransformCopy {

    private final FilterVarEqualityVisitor visitor;

    public TransformFilterVarEquality() {
        visitor = new FilterVarEqualityVisitor();
    }

    public Op transform(Op op) {
        return Transformer.transform(this, op, visitor, null);
    }


    @Override
    public Op transform(OpFilter opFilter, Op subOp) {
        if ( ! (subOp instanceof OpBGP) )
            return super.transform(opFilter, subOp);

        ExprList exprs = opFilter.getExprs();
        Op op = subOp;
        // Variables set
        Set<Var> patternVars = OpVars.visibleVars(op);

        // Any assignments must go inside filters so the filters see the assignments.
        ExprList exprs2 = new ExprList();

        for (  Expr e : exprs.getList() ) {
            Op op2 = processFilterWorker(e, op, patternVars);
            if ( op2 == null )
                exprs2.add(e);
            else
                op = op2;
        }

        // Place any filter expressions around the processed sub op.
        if ( exprs2.size() > 0 )
            op = OpFilter.filter(exprs2, op);
        return op;
    }


    private Op processFilterWorker(Expr e, Op subOp, Set<Var> patternVars) {
        if ( patternVars == null )
            patternVars = OpVars.visibleVars(subOp);
        // Rewrites:
        // FILTER ( ?x = ?y )

        if ( !(e instanceof E_Equals) )
            return null;

        // Corner case: sameTerm is false for string/plain literal,
        // but true in the graph for graphs with

        ExprFunction2 eq = (ExprFunction2)e;
        Expr left = eq.getArg1();
        Expr right = eq.getArg2();

        if ( left.isVariable() && right.isVariable() ) {
            // Both must be used or else.
            if ( patternVars.contains(left.asVar()) &&
                 patternVars.contains(right.asVar()) ) {
                if (visitor.projectVars != null && !visitor.projectVars.contains(right.asVar()))
                    return subst(subOp, left.getExprVar(), right.getExprVar());
                if (visitor.projectVars != null && !visitor.projectVars.contains(left.asVar()))
                    return subst(subOp, right.getExprVar(), left.getExprVar());
            }
        }

        return null;
    }


    private static Op subst(Op subOp , ExprVar var1, ExprVar var2) {
        // Replace var2 with var1
        Op op = Substitute.substitute(subOp, var2.asVar(), var1.asVar());
        // Insert LET(var2:=var1)
        return OpAssign.assign(op, var2.asVar(), var1);
    }

}
