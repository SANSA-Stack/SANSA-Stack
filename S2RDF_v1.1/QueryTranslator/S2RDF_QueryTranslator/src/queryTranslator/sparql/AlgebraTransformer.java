package queryTranslator.sparql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Stack;

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


import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.reasoner.rulesys.Node_RuleVariable;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpConditional;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpPath;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;
import com.hp.hpl.jena.sparql.algebra.op.OpSequence;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.path.Path;

/**
 *
 * @author Alexander Schaetzle
 */
public class AlgebraTransformer extends OpVisitorBase {

    private final Stack<SqlOp> stack;
    private final PrefixMapping prefixes;

    public AlgebraTransformer(PrefixMapping _prefixes) {
        stack = new Stack<SqlOp>();
        prefixes = _prefixes;
    }

    public SqlOp transform(Op op) {
        AlgebraWalker.walkBottomUp(this, op);
        return stack.pop();
    }


    
    @Override
    public void visit(OpBGP opBGP) {
        stack.push(new SqlBGP(opBGP, prefixes));
    }

    @Override
    public void visit(OpFilter opFilter) {
        SqlOp subOp = stack.pop();
        stack.push(new SqlFilter(opFilter, subOp, prefixes));
    }

    @Override
    public void visit(OpJoin opJoin) {
        SqlOp rightOp = stack.pop();
        SqlOp leftOp = stack.pop();
        stack.push(new SqlJoin(opJoin, leftOp, rightOp, prefixes));
    }
    /**
     * Visitor for the property path
     * @author Simon Skilevic
     */
    @Override
    public void visit(OpPath opPath) {
    	// Path subject
    	Node subject = opPath.getTriplePath().getSubject();
    	// Path object
    	Node object = opPath.getTriplePath().getObject();
    	// Property path 
    	Path tPath = opPath.getTriplePath().getPath();
    	
    	String stringPath = tPath.toString();
    	ArrayList<String> pathsStr = TransformerHelper.findAllPossiblePathes(stringPath);
    	int id=0;
    	for (String path:pathsStr){
    		BasicPattern pt = TransformerHelper.transformPathToBasicPattern(subject, path, object);
    		OpBGP opBGP = new OpBGP(pt);
        	stack.push(new SqlBGP(opBGP, prefixes));
    		id++;
    		if (id>1) {
    			 SqlOp rightOp = stack.pop();
    		     SqlOp leftOp = stack.pop();
    		     stack.push(new SQLUnion(null, leftOp, rightOp, prefixes));
    		}
    	}
    	
    }
    @Override
    public void visit(OpSequence opSequence) {
        SqlSequence sqlSequence = new SqlSequence(opSequence, prefixes);
        for(int i=0; i<opSequence.size(); i++) {
            sqlSequence.add(0,stack.pop());
        }
        stack.push(sqlSequence);
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin) {
        SqlOp rightOp = stack.pop();
        SqlOp leftOp = stack.pop();
        stack.push(new SqlLeftJoin(opLeftJoin, leftOp, rightOp, prefixes));
    }


    @Override
     public void visit(OpUnion opUnion) {
        SqlOp rightOp = stack.pop();
        SqlOp leftOp = stack.pop();
        stack.push(new SQLUnion(opUnion, leftOp, rightOp, prefixes));
    }

    @Override
    public void visit(OpProject opProject) {
        SqlOp subOp = stack.pop();
        stack.push(new SqlProject(opProject, subOp, prefixes));
    }

    @Override
    public void visit(OpDistinct opDistinct) {
        SqlOp subOp = stack.pop();
        stack.push(new SqlDistinct(opDistinct, subOp, prefixes));
    }

    @Override
    public void visit(OpOrder opOrder) {
        SqlOp subOp = stack.pop();
        stack.push(new SqlOrder(opOrder, subOp, prefixes));
    }

    @Override
    public void visit(OpSlice opSlice) {
        SqlOp subOp = stack.pop();
        // change order of tree nodes
        if(subOp instanceof SqlProject){
        	SqlSlice slice = new SqlSlice(opSlice, ((SqlProject) subOp).getSubOp(), prefixes);
        	SqlProject project = new SqlProject(((SqlProject) subOp).getOpProject(), (SqlOp)slice, prefixes);
        	stack.push(project);
        } else {
        stack.push(new SqlSlice(opSlice, subOp, prefixes));
        }
    }

    @Override
    public void visit(OpReduced opReduced) {
        SqlOp subOp = stack.pop();
        stack.push(new SqlReduced(opReduced, subOp, prefixes));
    }

}
