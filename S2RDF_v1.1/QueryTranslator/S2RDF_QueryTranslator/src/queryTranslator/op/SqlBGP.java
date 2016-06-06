package queryTranslator.op;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.print.attribute.standard.SheetCollate;

import queryTranslator.SqlOpVisitor;
import queryTranslator.SparkTableStatistics;
import queryTranslator.Tags;
import queryTranslator.sql.SqlStatement;
import queryTranslator.sql.SqlTriple;
import queryTranslator.sql.Join;
import queryTranslator.sql.JoinType;
import queryTranslator.sql.JoinUtil;
import queryTranslator.sql.Schema;
import queryTranslator.sql.SimpleTriple;
import queryTranslator.sql.TripleGroup;


import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;

/**
 * 
 * @author Antony Neu
 */
public class SqlBGP extends SqlOp0 {

	private final OpBGP opBGP;

	public SqlBGP(OpBGP _opBGP, PrefixMapping _prefixes) {
		super(_prefixes);
		opBGP = _opBGP;
		resultName = Tags.BGP;
	}
	
	/**
	 * Translates BGP object to SQL object
	 */
	public SqlStatement translate(String _resultName) {
		resultName = _resultName;
		
		List<Triple> triples = opBGP.getPattern().getList();

		HashMap<Node, TripleGroup> tripleGroups = new HashMap<Node, TripleGroup>();
		ArrayList<SqlTriple> trList = new ArrayList<SqlTriple>();

		// empty PrefixMapping when prefixes should be expanded
		if (expandPrefixes) {
			prefixes = PrefixMapping.Factory.create();
		}
		
		for (Triple triple : triples) {
			trList.add(new SimpleTriple(triple));
		}
		
		trList = Schema.initTriples(trList, prefixes, 3);

		ArrayList<SqlStatement> joinsList = new ArrayList<SqlStatement>();
		while (trList.size()>0)
		{
			SqlTriple first =  trList.get(0);
			trList.remove(0);			
			SqlStatement res = first.translate();
			res.setTabs(1);
			joinsList.add(res);
		}
		
		// generate candidate table lists
		SparkTableStatistics.generateCandidatesTableLists(joinsList);		
		SparkTableStatistics.determineBestCandidateTable();
		
		// calculate best joining order
		joinsList = JoinUtil.sortJoinList(joinsList);
		
		SqlStatement firstStatment = joinsList.get(0);
		joinsList.remove(0);

		// if joinList is not empty after we removed first subquery
		// -> we have to join other SqlStatments to the first one.
		if (joinsList.size() > 0){
			
			// joinList is already sorted -> start with the first subquery and
			// join in every loop a further subquery which appears first in 
			// joinList order and has at least on common varibale with already 
			// joined subqueries

			Join join=null;
			int id=-1;

			while (joinsList.size() > 0) {
				id++;
				ArrayList<String> onConditions = new ArrayList<String>();
				ArrayList<SqlStatement> rights = new ArrayList<SqlStatement>();
				Map<String, String[]> group_shifted = Schema.shiftToParent(firstStatment.getVariables(), firstStatment.getName());				
				
				while (joinsList.size() > 0){
					int index = JoinUtil.findJoinPartner(group_shifted, joinsList);
					SqlStatement right = joinsList.get(index);
					Map<String, String[]> right_shifted = Schema.shiftToParent(right.getVariables(), right.getName());
					onConditions.add(JoinUtil.generateConjunction(JoinUtil
							.getOnConditions(group_shifted, right_shifted)));
					this.resultName = "tab"+Schema.getTableId();					
					rights.add(right);
					group_shifted.putAll(right_shifted);
					joinsList.remove(index);
				}
				
				join = new Join(this.resultName, firstStatment, rights,
						onConditions, JoinType.natural);
				join.setTabs(joinsList.size() - id);				
										

			};
			
			this.resultSchema = join.getMappings();
			return join;
		}
		
		this.resultName = firstStatment.getName();
		SqlStatement res = firstStatment;		
		this.resultSchema = res.getMappings();

		// TODO: remove if no more needed 
		// debugging information 
		/*for (String key:this.resultSchema.keySet())
		{
			System.out.println("Schema-->"+key+"<->"+this.resultSchema.get(key)[0]);
		}
		System.out.println("BGP->"+res.toString());*/
		return res;
	}

	@Override
	public void visit(SqlOpVisitor sqlOpVisitor) {
		sqlOpVisitor.visit(this);
	}

}