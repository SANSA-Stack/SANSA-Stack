/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */
package queryTranslator;

import java.util.ArrayList;
/**
 * An object of this class corresponds to a list of candidates, which can be 
 * deployed for usage in SQL query for certain table in it.
 * 
 * An object contains candidates list, an id of the best candidate and 
 * a place holder name, which marks the place in SQL query, where the best
 * candidate can be inserted
 * 
 * @author Simon Skilevic
 *
 */
public class SqlTableCandidatesList {	

	// place holder for the table, which gonna be used in SQL query 
	public String placeHolderName = "";
	public String placeHolderId = "";
	// best table (actual: table with the smallest size)
	public int bestCandidateID=0;
	
	// candidate tables for the usage in SQL query
	// Every candidate table is stored as String-array with format: 
	// {[relation type], [pred1], [pred2]} for ExtVP table
	// {"VP", [pred1], ""} for VP table
	private ArrayList<String[]> sqlTableCandidates = new ArrayList<String[]>();
	
	public SqlTableCandidatesList(String tableName){		
		
		// (simon) Terible,terible hack to solve problem with <entity> like entities
		// base prefix has to be removed
		tableName = tableName.replace("http__//yago-knowledge.org/resource/", "");
		
		Tags.PLACE_HOLDER_ID++;
		placeHolderId = ""+Tags.PLACE_HOLDER_ID;
		//this.placeHolderName = tableName.replace("<", "_L_").replace(">", "_B_")+"$$"+placeHolderId+"$$";
		this.placeHolderName = tableName +"$$"+placeHolderId+"$$";
		// Add VP table as a default candidate for usage in SQL query
		sqlTableCandidates.add(new String[]{"VP", tableName, ""});
	}
	/**
	 * Add new ExtVP table candidate, which is corresponded to given relation
	 * of TP(?, pred1, ?) to (?, pred2, ?) 
	 * @param relType Relation Type (SO,OS,SS)
	 * @param pred1 Predicate of main triple pattern
	 * @param pred2 Predicate of slave triple pattern
	 */
	public void addExtVPCandidateTable(String relType, String pred1, String pred2){
		
		// check if ExtVP tables of given relation type can be used in SQL query
		if (relType.equals("SO") && !Tags.ALLOW_SO) return;
		if (relType.equals("OS") && !Tags.ALLOW_OS) return;
		if (relType.equals("SS") && !Tags.ALLOW_SS) return;
		
		sqlTableCandidates.add(new String[]{relType, pred1, pred2});
	}
	/**
	 * Get all candidates for the SQL table 
	 * @return list of candidates for usage in SQL query
	 */
	public ArrayList<String[]> getSqlTableCandidates(){
		return sqlTableCandidates;
	}
}
