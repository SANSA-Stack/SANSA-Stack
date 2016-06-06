package queryTranslator.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.jena.atlas.lib.NotImplemented;

public class Union extends SqlStatement {

	private SqlStatement left;
	private SqlStatement right;
	private ArrayList<String> _variables;
	
	public Union(String tablename, SqlStatement left, SqlStatement right) {
		super(tablename);
		this.left = left;
		this.right = right;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("(");
		sb.append(Schema.removeScopes(left.toString()));
		sb.append(" UNION ALL ");
		sb.append(Schema.removeScopes(right.toString()));
		sb.append(")");
		return sb.toString();
	}

	// TODO
	@Override
	public void addSelector(String alias, String[] selector) {
		left.addSelector(alias, selector);
		right.addSelector(alias, selector);
	}

	
	
	@Override
	public HashMap<String, String[]> getSelectors() {
		return left.getSelectors();
	}

	@Override
	public void addConjunction(String where) {
		left.addConjunction(where);
		right.addConjunction(where);
	}

	@Override
	public void addOrder(String byColumn) {
		left.addOrder(byColumn);
		right.addOrder(byColumn);
		
	}

	@Override
	public void updateSelection(Map<String, String[]> resultSchema) {
		left.updateSelection(resultSchema);
		right.updateSelection(resultSchema);
		
	}

	@Override
	public void removeNullFilters() {
		left.removeNullFilters();
		right.removeNullFilters();	
	}

	@Override
	public void addLimit(int i) {
		left.addLimit(i);
		right.addLimit(i);
	}

	@Override
	public boolean addOffset(int i) {
		// TODO Auto-generated method stub
		throw new NotImplemented();
	}

	@Override
	public String getOrder() {
		return left.getOrder();
	}

	@Override
	public void setVariables(ArrayList<String> vars) {
		// TODO Auto-generated method stub
		_variables = vars;
	}

	@Override
	public ArrayList<String> getVariables() {
		// TODO Auto-generated method stub
		return _variables;
	}

	@Override
	public Map<String, String[]> getMappings() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setMappings(HashMap<String, String[]> sel) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public String getType() {
		// TODO Auto-generated method stub
		return "Union";
	}

	@Override
	public HashMap<String, String> getAliasToColumn() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getFrom() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setFrom(String from) {
		// TODO Auto-generated method stub
		
	}


}
