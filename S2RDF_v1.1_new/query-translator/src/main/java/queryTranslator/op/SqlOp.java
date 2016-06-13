package queryTranslator.op;

import java.util.ArrayList;
import java.util.Map;

import queryTranslator.SqlOpVisitor;


/**
 *
 * @author Antony Neu
 */
public interface SqlOp {
    public Map<String, String[]> getSchema();
    public String getResultName();
    public void setResultName(String _resultName);
    public boolean getExpandMode();
    public void setExpandMode(boolean _expandPrefixes);
    public void visit(SqlOpVisitor sqlOpVisitor);

}
