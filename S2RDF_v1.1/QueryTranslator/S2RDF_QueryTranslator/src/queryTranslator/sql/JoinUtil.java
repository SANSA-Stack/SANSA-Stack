package queryTranslator.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.jena.atlas.lib.NotImplemented;

import queryTranslator.SparkTableStatistics;
import queryTranslator.TStat;


import com.hp.hpl.jena.sparql.engine.http.Params.Pair;

public class JoinUtil {

	/** 
	 * Returns id of the first SqlStatement, which has common variable with 
	 * already joined subqueries.
	 */
	public static int findJoinPartner(Map<String, String[]> group_shifted, ArrayList<SqlStatement> gs) {
		int index = 0;
		Map<String, String[]> leftVars = new HashMap<String, String[]>();
		leftVars.putAll(group_shifted);
		for (int i = 0; i < gs.size(); i++){ 			
			int sharedVars = JoinUtil.getSharedVars(leftVars, gs.get(i).getMappings()).size();
			if(sharedVars > 0){
				return i;
			}
		}
		return index;
	}
	
	public static ArrayList<String> getSharedVars(
			Map<String, String[]> leftSchema, Map<String, String[]> rightSchema) {
		ArrayList<String> result = new ArrayList<String>();
		for (String key : rightSchema.keySet()) {
			if (leftSchema.containsKey(key))
				result.add(key);
		}
		return result;
	}
	
	public static List<String> getOnConditions(Map<String, String[]> leftSchema,
			Map<String, String[]> rightSchema) {
		ArrayList<String> conditions = new ArrayList<String>();
		ArrayList<String> sharedVars = getSharedVars(leftSchema, rightSchema);
		if (sharedVars.size() == 0) {
			System.err.println("Warning! Found cross product!");
			return new ArrayList();
		} else {

			for (String var : sharedVars) {
				conditions.add(leftSchema.get(var)[0] + "." + leftSchema.get(var)[1]  + "="
						+ rightSchema.get(var)[0] + "." + rightSchema.get(var)[1]);
			}
		}
		return conditions;
	}
	
	/**
	 * Generate conjunctions of given conditions
	 * @param conditions list
	 * @return conjunctions String
	 */
	public static String generateConjunction(List<String> conditions){
		String result = "";
		
		boolean first = true;
		for (String cond : conditions) {
			if (!first) {
				result += " AND ";
			} else {
				first = false;
			}
			result += cond;

		}
		return result;
	}
	/**
	 * Sort the joining order of SQL statements
	 * Sorting Order:
	 * Main sorting parameter is the number of defined by values TP elements (more -> better)
	 * Second sorting parameter is the size of table correpoded to SQL statement (smaller -> better)
	 * 
	 * @author Simon Skilevic 
	 *      
	 * @param joinList list of SQL subqueries, which have to be joined
	 * @return sorted list of SQL subqueries
	 */
	public static ArrayList<SqlStatement> sortJoinList(ArrayList<SqlStatement> joinList){
		
		Collections.sort(joinList, new Comparator<SqlStatement>(){
			@Override
			public int compare(SqlStatement st1, SqlStatement st2){
				if (st1.getVariables().size() == st2.getVariables().size()){				
					TStat stat1 = SparkTableStatistics.getBestCandidateTableStatistic(st1.getFrom());
					TStat stat2 = SparkTableStatistics.getBestCandidateTableStatistic(st2.getFrom());					
					if (stat2.size > stat1.size) return -1;
					if (stat2.size < stat1.size) return 1;
					return 0;
				} else {
					return st1.getVariables().size() - st2.getVariables().size();
				}
			}
		});
		
		return joinList;
	}

}
