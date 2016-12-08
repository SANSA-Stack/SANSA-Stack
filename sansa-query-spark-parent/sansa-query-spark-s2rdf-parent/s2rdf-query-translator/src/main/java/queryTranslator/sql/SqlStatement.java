package queryTranslator.sql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class SqlStatement {

	protected String statementName;
	protected boolean isDistinct = false;
	protected int tabs = 0;

	public String getName() {
		return statementName;
	}

	public SqlStatement(String name) {
		this.statementName = name;
	}

	public abstract void addConjunction(String where);

	public abstract void addOrder(String byColumn);
	
	public abstract String getOrder();

	public abstract String toString();

	/**
	 * 
	 * @return String represantation with item name, e.g. (SELECT ...) table1.
	 */
	public String toNamedString() {
		return this.toString() + " " + statementName;
	}

	public abstract void addSelector(String alias, String[] selector);

	public abstract HashMap<String, String[]> getSelectors();

	public abstract void updateSelection(Map<String, String[]> resultSchema);

	
	public abstract void addLimit(int i);
	public abstract boolean addOffset(int i);
	
	public void setDistinct(boolean b) {
		this.isDistinct = b;
	}

	public abstract void removeNullFilters();
	public abstract void setVariables(ArrayList<String> vars);
	public abstract ArrayList<String> getVariables();
	public abstract Map<String, String[]> getMappings();
	public abstract void setMappings(HashMap<String, String[]> sel);
	public abstract String getType();
	public void setTabs(int tabNum){ 
		this.tabs = tabNum;
	};
	public int getTabs(){
		return tabs;
	}
	public abstract HashMap<String, String> getAliasToColumn();
	public abstract String getFrom();
	public abstract void setFrom(String from);
}
