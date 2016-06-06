package queryTranslator.sparql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import javax.naming.spi.DirStateFactory.Result;

import queryTranslator.Tags;


import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.expr.E_Add;
import com.hp.hpl.jena.sparql.expr.E_Bound;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.E_Function;
import com.hp.hpl.jena.sparql.expr.E_GreaterThan;
import com.hp.hpl.jena.sparql.expr.E_GreaterThanOrEqual;
import com.hp.hpl.jena.sparql.expr.E_Lang;
import com.hp.hpl.jena.sparql.expr.E_LangMatches;
import com.hp.hpl.jena.sparql.expr.E_LessThan;
import com.hp.hpl.jena.sparql.expr.E_LessThanOrEqual;
import com.hp.hpl.jena.sparql.expr.E_LogicalAnd;
import com.hp.hpl.jena.sparql.expr.E_LogicalNot;
import com.hp.hpl.jena.sparql.expr.E_LogicalOr;
import com.hp.hpl.jena.sparql.expr.E_NotEquals;
import com.hp.hpl.jena.sparql.expr.E_Regex;
import com.hp.hpl.jena.sparql.expr.E_Str;
import com.hp.hpl.jena.sparql.expr.E_Subtract;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprFunction;
import com.hp.hpl.jena.sparql.expr.ExprFunction0;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprFunction3;
import com.hp.hpl.jena.sparql.expr.ExprFunctionN;
import com.hp.hpl.jena.sparql.expr.ExprFunctionOp;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.ExprVisitor;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.util.FmtUtils;

/**
 *
 * @author Alexander Schaetzle
 */
public class ExprTranslator implements ExprVisitor {

    private boolean expandPrefixes;
    private final Stack<String> stack;
    private final PrefixMapping prefixes;
	private Map<String, String[]> schema;
	
	public ExprTranslator(PrefixMapping _prefixes) {
        stack = new Stack<String>();
        prefixes = _prefixes;
    }

    public String translate(Expr expr, boolean expandPrefixes, Map<String, String[]> resultSchema) {
    	this.schema = resultSchema;
        this.expandPrefixes = expandPrefixes;
        ExprWalker.walkBottomUp(this, expr);
        if(stack.isEmpty())
        	return "";
        return stack.pop();
    }


    
    @Override
    public void startVisit() { }
    
    public void visit(ExprFunction func) {
        if (func instanceof ExprFunction0) {
            visit((ExprFunction0) func);
        }
        else if (func instanceof ExprFunction1) {
            visit((ExprFunction1) func);
        }
        else if (func instanceof ExprFunction2) {
            visit((ExprFunction2) func);
        }
        else if (func instanceof ExprFunction3) {
            visit((ExprFunction3) func);
        }
        else if (func instanceof ExprFunctionN) {
            visit((ExprFunctionN) func);
        }
        else if (func instanceof ExprFunctionOp) {
            visit((ExprFunctionOp) func);
        }
    }

    @Override
    public void visit(ExprFunction1 func) {
        boolean before = true;
        String sub = stack.pop();

        String operator = Tags.NO_SUPPORT;
        if (func instanceof E_LogicalNot) {
            if (func.getArg() instanceof E_Bound) {
                operator = Tags.NOT_BOUND;
                sub = sub.substring(0, sub.indexOf(Tags.BOUND));
                before = false;
            }
            else {
                operator = Tags.LOGICAL_NOT;
            }
        }
        else if (func instanceof E_Bound) {
            operator = Tags.BOUND;
            before = false;
        } else if (func instanceof E_Str || func instanceof E_Lang){
        	operator = "";
        }

        if (operator.equals(Tags.NO_SUPPORT)) {
            throw new UnsupportedOperationException("Filter expression not supported yet!");
        }
        else {
            if(schema.containsKey(sub)){
            	sub = schema.get(sub)[0]+"."+sub;
            }
            if (before) {
                stack.push( operator + sub );
                
            }
            else {
                stack.push(sub + operator );
            }
        }
    }

    @Override
    public void visit(ExprFunction2 func) {
        String right = stack.pop();
        String left = stack.pop();

        if(schema.containsKey(left)){
        	String[] entry = schema.get(left);
        	if(entry.length > 1){
        		left = entry[0]+"."+entry[1];
        	} else {
        		left = entry[0];
        	}
        }

        if(schema.containsKey(right)){
        	String[] entry = schema.get(right);
        	if(entry.length > 1){
        		right = entry[0]+"."+entry[1];
        	} else {
        		right = entry[0];
        	}
        }
        
        
        String operator = Tags.NO_SUPPORT;
        if (func instanceof E_GreaterThan) {
            operator = Tags.GREATER_THAN;
        }
        else if (func instanceof E_GreaterThanOrEqual) {
            operator = Tags.GREATER_THAN_OR_EQUAL;
        }
        else if (func instanceof E_LessThan) {
            operator = Tags.LESS_THAN;
        }
        else if (func instanceof E_LessThanOrEqual) {
            operator = Tags.LESS_THAN_OR_EQUAL;
        }
        else if (func instanceof E_Equals) {
            operator = Tags.EQUALS;
        }
        else if (func instanceof E_NotEquals) {
            operator = Tags.NOT_EQUALS;
        }
        else if (func instanceof E_LogicalAnd) {
            operator = Tags.LOGICAL_AND;
        }
        else if (func instanceof E_LogicalOr) {
            operator = Tags.LOGICAL_OR;
        } else if(func instanceof E_Add){
        	operator = Tags.ADD;
        }else if(func instanceof E_Subtract){
        	operator = Tags.SUBTRACT;
        } else if (func instanceof E_LangMatches){
        	operator = Tags.LANG_MATCHES;
        }

        if (operator.equals(Tags.NO_SUPPORT)) {
            throw new UnsupportedOperationException("Filter expression not supported yet!");
        }  else if(operator.equals(Tags.LANG_MATCHES)){
        	right = "%@" + right.split("\"")[1];
        	stack.push("(" + left + operator + "'"+ right + "'"+ ")");
        } else {
            stack.push("(" + left.replace("\"'", "'").replace("'\"", "'") + operator + right.replace("\"'", "'").replace("'\"", "'") + ")");
        }
    }

    @Override
    public void visit(NodeValue nv) {
    	try {
    		stack.push(""+Integer.parseInt(FmtUtils.stringForNode(nv.asNode())));
    	} catch (NumberFormatException e) {
    		String value = FmtUtils.stringForNode(nv.asNode(), prefixes);
    		if(value.contains("\"")){
    			value.replace("\"", "");
    		}
    		stack.push("'"+value+"'");
    	}
    	
        
    }

    @Override
    public void visit(ExprVar nv) {
        stack.push(nv.getVarName());
    }

    @Override
    public void visit(ExprFunction0 func) {
        throw new UnsupportedOperationException("ExprFunction0 not supported yet.");
    }

    @Override
    public void visit(ExprFunction3 func) {
        throw new UnsupportedOperationException("ExprFunction3 not supported yet.");
    }
    
    @Override
    public void visit(ExprFunctionN func) {
    if(func instanceof E_Regex){
    	String operator = Tags.NO_SUPPORT;
        String right = stack.pop();
        String left = stack.pop();
        
        if(schema.containsKey(left)){
        	String[] entry = schema.get(left);
        	if(entry.length > 1){
        		left = entry[0]+"."+entry[1];
        	} else {
        		left = entry[0];
        	}
        }

        if(schema.containsKey(right)){
        	String[] entry = schema.get(right);
        	if(entry.length > 1){
        		right = entry[0]+"."+entry[1];
        	} else {
        		right = entry[0];
        	}
        }
        
        operator = Tags.LIKE;
    	stack.push("(" + left + operator + right + ")");
    } else if(func instanceof E_Function){
    	System.out.println("Unknown function found.");
    }
    else{
        throw new UnsupportedOperationException("ExprFunctionN not supported yet!");
    }
    }

    @Override
    public void visit(ExprFunctionOp funcOp) {
        throw new UnsupportedOperationException("ExprFunctionOp not supported yet.");
    }

    @Override
    public void visit(ExprAggregator eAgg) {
        throw new UnsupportedOperationException("ExprAggregator not supported yet.");
    }

    @Override
    public void finishVisit() { }

}
