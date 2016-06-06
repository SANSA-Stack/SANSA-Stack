package queryTranslator.sparql;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.TransformCopy;
import com.hp.hpl.jena.sparql.algebra.Transformer;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderFixed;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderLib;

/**
 *
 * @author Alexander Schaetzle
 */
public class BGPOptimizerNoStats extends TransformCopy {

    public BGPOptimizerNoStats() {}

    public Op optimize(Op op) {
        return Transformer.transform(this, op);
    }


    @Override
    public Op transform(OpBGP opBGP) {
        // if there are no more than 2 Triples -> reordering is useless
        if (opBGP.getPattern().size() <= 2) {
            return opBGP;
        }

        // Reorder by Selectivity
        ReorderFixed optimizer1 = (ReorderFixed) ReorderLib.fixed();
        BasicPattern optimizedPattern1 = optimizer1.reorder(opBGP.getPattern());

        // Reorder to avoid cross products and reduce the number of joins, if possible
        ReorderNoCross optimizer2 = new ReorderNoCross();
        BasicPattern optimizedPattern2 = optimizer2.reorder(optimizedPattern1);

        OpBGP optimizedBGP = new OpBGP(optimizedPattern2);
        return optimizedBGP;

        /*
        Heuristic variableCountingUnbound = new VariableCountingUnbound();
        BasicPatternGraph graph = new BasicPatternGraph(opBGP.getPattern(), variableCountingUnbound);
        BasicPattern optimizedPattern2 = graph.optimize();
        OpBGP optimizedBGP2 = new OpBGP(optimizedPattern2);
        return optimizedBGP2;
         */
    }

}
