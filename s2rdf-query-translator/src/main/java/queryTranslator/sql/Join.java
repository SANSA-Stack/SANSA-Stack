package queryTranslator.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.atlas.lib.NotImplemented;

public class Join extends SqlStatement {

	private SqlStatement left;
	private List<SqlStatement> rights;
	private Select wrapper;
	private List<String> onStrings;
	private JoinType type = JoinType.natural;
	private ArrayList<String> _variables;
	
	public Join(String tablename, SqlStatement left, List<SqlStatement> rights,
			List<String> onStrings, JoinType type) {
		super(tablename);
		this.left = left;
		this.rights = rights;
		this.type = type;
		this.onStrings = onStrings;

		// add selectors only once
		HashSet<String> added = new HashSet<String>();
		this.wrapper = new Select(tablename);
		for (String key: left.getSelectors().keySet()) {
			String[] selector = left.getSelectors().get(key);
			String name = "";
			if(selector.length > 1){
				name = selector[1];
			} else{
				name = selector[0];
			}
			if (!added.contains(key)) {
				added.add(key);
				wrapper.addSelector(key, new String[] { left.getName() ,key });
			}
		}
		for (SqlStatement right : rights) {
			for (String key: right.getSelectors().keySet()) {
				String[] selector = right.getSelectors().get(key);
				String name = "";
				if(selector.length > 1){
					name = selector[1];
				} else{
					name = selector[0];
				}
				if (!added.contains(key)) {
					added.add(key);
					wrapper.addSelector(key, new String[] { right.getName(), key });
				}
			}
		}
	}
	@Override
	public String toString() {
		wrapper.setTabs(tabs);
		StringBuilder sb = new StringBuilder("");
		sb.append(left.toNamedString());
		for (int i = 0; i < rights.size(); i++) {
			SqlStatement right = rights.get(i);
			String onString = onStrings.get(i);
			if(onString.equals("") || this.type == JoinType.cross){
				sb.append("\n"+Schema.writeTabs(getTabs())+" CROSS JOIN ");
			}
			else if(this.type == JoinType.left_outer){
			sb.append("\n"+Schema.writeTabs(getTabs())+" LEFT OUTER JOIN ");
			} else{
				sb.append("\n"+Schema.writeTabs(getTabs())+" JOIN ");	
			}
			sb.append(right.toNamedString());
			if(!onString.equals("") && this.type != JoinType.cross){
				sb.append("\n"+Schema.writeTabs(getTabs())+" ON(");
				sb.append(onString);
				sb.append(")");
			} else if(!onString.equals("") && this.type == JoinType.cross){
				wrapper.addConjunction(onString);
			}
		}
		wrapper.setFrom(sb.toString());

		return wrapper.toString();
	}

	// TODO
	@Override
	public void addSelector(String alias, String[] selector) {
		wrapper.addSelector(alias, selector);

	}

	@Override
	public HashMap<String, String[]> getSelectors() {
		return wrapper.getSelectors();
	}

	@Override
	public void addConjunction(String where) {
		wrapper.addConjunction(where);
		
	}

	@Override
	public void addOrder(String byColumn) {
		wrapper.addOrder(byColumn);
	}

	@Override
	public void updateSelection(Map<String, String[]> resultSchema) {
		wrapper.updateSelection(resultSchema);
		
	}

	@Override
	public void removeNullFilters() {
		wrapper.removeNullFilters();
//		left.removeNullFilters();
//		for(SQLStatement right: rights){
//			right.removeNullFilters();
//		}
	}

	@Override
	public void addLimit(int i) {
		wrapper.addLimit(i);
		
	}

	@Override
	public boolean addOffset(int i) {
		return wrapper.addOffset(i);
	}

	@Override
	public String getOrder() {
		return this.wrapper.getOrder();
	}
	@Override
	public void setVariables(ArrayList<String> vars) {
		// TODO Auto-generated method stub
		_variables = vars;
	}

	@Override
	public ArrayList<String> getVariables() {
		// TODO Auto-generated method stub
		Set<String> result = new HashSet<String>();
		result.addAll(left.getVariables());
		for (SqlStatement st:rights){
			result.addAll(st.getVariables());
		}
		ArrayList<String> listRes = new ArrayList<String>();
		listRes.addAll(result);
		return listRes;
	}

	@Override
	public Map<String, String[]> getMappings() {
		/*Map<String, String[]> res = new HashMap<String, String[]>();
		for (String var:getVariables()){
			if (getName()!=null)
				res.put(var, new String[]{getName() ,var});
			else
				res.put(var, new String[]{var});
		}
		return res;*/
		return wrapper.getMappings();
	}
	
	@Override
	public void setMappings(HashMap<String, String[]> sel) {
		wrapper.setMappings(sel);
	}
	
	public void setDistinct(){
		wrapper.isDistinct=true;
	}
	@Override
	public String getType() {
		// TODO Auto-generated method stub
		return "Join";
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
