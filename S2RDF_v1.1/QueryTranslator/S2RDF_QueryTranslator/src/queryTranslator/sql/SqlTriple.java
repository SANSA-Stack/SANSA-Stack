package queryTranslator.sql;

import java.util.ArrayList;
import java.util.Map;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;


public interface SqlTriple {

	public String getType();
	public Triple getFirstTriple();
	public Triple getSecondTriple();
	public SqlStatement translate();
	public void setTableName(String tName);
	public String getTableName();
	public void setPrefixMapping(PrefixMapping pMapping);
	public ArrayList<String> getVariables();
	public Map<String, String[]> getMappings();
	public int getTabs();
	public void setDistinct();
	public void setTabs(int tabs);
	public int getNumberOfValues();
}
