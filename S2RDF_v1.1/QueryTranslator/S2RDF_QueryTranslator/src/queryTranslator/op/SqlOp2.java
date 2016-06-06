package queryTranslator.op;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import queryTranslator.sql.SqlStatement;
import queryTranslator.sql.Select;


import com.hp.hpl.jena.shared.PrefixMapping;

/**
 *
 * @author Antony Neu
 */
public abstract class SqlOp2 extends SqlBase {

    protected SqlOp leftOp, rightOp;

    
    protected SqlOp2(SqlOp _leftOp, SqlOp _rightOp, PrefixMapping _prefixes) {
        leftOp = _leftOp;
        rightOp = _rightOp;
        prefixes = _prefixes;
        resultSchema = new HashMap<String, String[]>();
    }
    
    public abstract SqlStatement translate(String name, SqlStatement left, SqlStatement right);

    public SqlOp getLeft() {
        return leftOp;
    }

    public SqlOp getRight() {
        return rightOp;
    }

/*
    protected ArrayList<String> getSharedVars() {
        Map<String, String[]> leftOpSchema = leftOp.getSchema();
        Map<String, String[]> rightOpSchema = rightOp.getSchema();
        ArrayList<String> sharedVars = new ArrayList<String>();

        for(String leftKey : leftOpSchema.keySet()) {
            if(rightOpSchema.containsKey(leftKey)) {
                sharedVars.add(leftKey);
            }
        }
        return sharedVars;
    }

    
    protected ArrayList<String> getOnlyLeftVars(){
        Map<String, String[]> leftOpSchema = leftOp.getSchema();
        Map<String, String[]> rightOpSchema = rightOp.getSchema();
        ArrayList<String> vars = new ArrayList<String>();

        for(String leftKey : leftOpSchema.keySet()) {
            if(!rightOpSchema.containsKey(leftKey)) {
            	vars.add(leftKey);
            }
        }
        return vars;
    }

    
    
    protected ArrayList<String> getOnlyRightVars(){
        Map<String, String[]> leftOpSchema = leftOp.getSchema();
        Map<String, String[]> rightOpSchema = rightOp.getSchema();
        ArrayList<String> vars = new ArrayList<String>();

        for(String leftKey : leftOpSchema.keySet()) {
            if(!rightOpSchema.containsKey(leftKey)) {
            	vars.add(leftKey);
            }
        }
        return vars;
    }

 */

}
