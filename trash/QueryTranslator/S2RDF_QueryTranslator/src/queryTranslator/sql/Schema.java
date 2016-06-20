package queryTranslator.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.swing.text.html.HTML.Tag;

import queryTranslator.Tags;


import com.hp.hpl.jena.shared.PrefixMapping;

public class Schema {

	private static int _tableId;
	public static int getTableId(){
		return _tableId++;
	}

	public static Map<String, String[]> shiftToParent(
			Map<String, String[]> original, String table) {
		HashMap<String, String[]> result = new HashMap<String, String[]>();
		for (String key : original.keySet()) {
			String[] entry = original.get(key);
			result.put(key, new String[] { table, key });

		}
		return result;
	}
	
	public static String removeScopes(String statment){
		return statment.substring(statment.indexOf('(')+1, statment.lastIndexOf(')'));
	}
	
	public static String writeTabs(int num){
		String res="";
		for (int i = 0; i < num; i++){
			res+="\t";
		}
		return res;
	}
	public static Map<String, String[]> shiftToParent(
			ArrayList<String> original, String table) {
		HashMap<String, String[]> result = new HashMap<String, String[]>();
		for (String key : original) {
			result.put(key, new String[] { table, key });

		}
		return result;
	}

	public static Map<String, String[]> shiftOrigin(
			Map<String, String[]> original, String table) {
		HashMap<String, String[]> result = new HashMap<String, String[]>();
		for (String key : original.keySet()) {
			String[] entry = original.get(key);
			if (entry.length == 2)
				result.put(key, new String[] { table, entry[1] });
			else
				result.put(key, new String[] { table, entry[0] });

		}
		return result;
	}
	
	public static ArrayList<SqlTriple> initTriples(ArrayList<SqlTriple> list, PrefixMapping pMapping, int tabs){
		for (int i = 0; i < list.size(); i++){
			list.get(i).setTableName(Tags.BGP+getTableId());
			list.get(i).setPrefixMapping(pMapping);
			list.get(i).setTabs(tabs);
		}
		return list;
	}
	

}
