package queryTranslator.op;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.hp.hpl.jena.shared.PrefixMapping;

/**
 *
 * @author Antony Neu
 */
public abstract class SqlBase implements SqlOp {

    protected PrefixMapping prefixes;
    protected boolean expandPrefixes;
    protected String resultName;
    
    protected Map<String, String[]> resultSchema;

    @Override
    public Map<String, String[]> getSchema() {
		return resultSchema;
	}

    @Override
    public String getResultName() {
        return resultName;
    }

    @Override
    public void setResultName(String _resultName) {
        resultName = _resultName;
    }

    @Override
    public boolean getExpandMode() {
        return expandPrefixes;
    }

    @Override
    public void setExpandMode(boolean _expandPrefixes) {
        expandPrefixes = _expandPrefixes;
    }
    
   



    protected String toArgumentList(ArrayList<String> list) {
        if(list == null || list.isEmpty()) return null;
        String argList = "";
        if(list.size() == 1) return list.get(0);
        else {
            argList += "(" + list.get(0);
            for (int i=1; i<list.size(); i++) {
                argList += ", " + list.get(i);
            }
            argList += ")";
        }
        return argList;
    }

   
    
    
    protected ArrayList<String> getVarsOnlnyInOp(SqlBase other){
    	ArrayList<String> result = new ArrayList<String>();
    	for(String key : this.getSchema().keySet()){
    		if(!other.getSchema().containsKey(key)){
    			result.add(key);
    		}
    	}
    	
    	
    	return result;
    }

}
