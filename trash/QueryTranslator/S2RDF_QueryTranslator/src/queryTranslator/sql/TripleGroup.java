package queryTranslator.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.atlas.lib.NotImplemented;

import queryTranslator.Tags;


import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.util.FmtUtils;

/**
 * Group of triples within a BGP.
 * 
 * @author neua
 * 
 */
public class TripleGroup {

	private List<Triple> triples = new ArrayList<Triple>();

	private String name;

	private int subQueries = 0;

	public String getName() {
		return name;
	}

	/**
	 * If the same predicate is selected twice, then a cross join is needed.
	 */

	ArrayList<Triple> crossjoin = new ArrayList<Triple>();
	private Map<String, String[]> crossJoinMapping = new HashMap<String, String[]>();

	// choose triplestore as predicate unbound
	private boolean selectPredicateOnlny = false;

	PrefixMapping prefixMapping;

	private Map<String, String[]> mapping = new HashMap<String, String[]>();

	public TripleGroup(String tablename, PrefixMapping mapping) {
		this(tablename, mapping, false);
	}

	public TripleGroup(String tablename, PrefixMapping mapping,
			 boolean selectPredicateOnlny) {
		this.name = tablename;
		this.prefixMapping = mapping;
		this.selectPredicateOnlny = selectPredicateOnlny;
	}

	public void add(Triple triple) {
		if (!searchTripleSamePredicate(triple)) {
			triples.add(triple);
			mapping.putAll(getMappingVarsOfTriple(triple));
		} else {
			crossjoin.add(triple);
			crossJoinMapping.putAll(getMappingVarsOfTriple(triple));
		}

	}

	private HashMap<String, String[]> getMappingVarsOfTriple(Triple t) {
		HashMap<String, String[]> result = new HashMap<String, String[]>();
		Node subject = t.getSubject();
		Node predicate = t.getPredicate();
		Node object = t.getObject();
		if (subject.isVariable())
			result.put(subject.getName(),
					new String[] { Tags.SUBJECT_COLUMN_NAME });
		if (predicate.isVariable()) {
			result.put(predicate.getName(),
					new String[] { Tags.PREDICATE_COLUMN_NAME });
		}
		if (object.isVariable()) {
			result.put(object.getName(),
					new String[] { Tags.OBJECT_COLUMN_NAME });
		}
		return result;
	}
	
	public SqlStatement translate() {
		Select select = new Select(this.name);

		ArrayList<String> vars = new ArrayList<String>();
		ArrayList<String> whereConditions = new ArrayList<String>();
		boolean first = true;
		for (int i = 0; i < triples.size(); i++) {
			Triple triple = triples.get(i);
			Node subject = triple.getSubject();
			Node predicate = triple.getPredicate();
			Node object = triple.getObject();

			if (first) {
				first = false;
				// only check subject once per group
				if (subject.isURI() || subject.isBlank()) {
					// subject is bound -> add to Filter
					whereConditions.add("ID = '"
							+ FmtUtils.stringForNode(subject,
									this.prefixMapping) + "'");
				} else {
					vars.add(subject.getName());
					whereConditions.add(Tags.SUBJECT_COLUMN_NAME
							+ " is not null ");
				}
			}
			if (predicate.isURI()) {
				// cross join needed?
				int index = searchTripleSamePredicate(i);
				while (index != -1) {
					crossjoin.add(triples.get(index));
					triples.remove(index);
					index = searchTripleSamePredicate(i);
				}
				// predicate is bound -> add to Filter
				whereConditions.add(SpecialCharFilter.filter(FmtUtils
						.stringForNode(predicate, this.prefixMapping)
						+ " is not null"));

			} else {
				vars.add(predicate.getName());
			}
			if (object.isURI() || object.isLiteral() || object.isBlank()) {
				String string = FmtUtils.stringForNode(object,
						this.prefixMapping);
				if (object.isLiteral()) {
					string = "" + object.getLiteral().getValue();
				}
				String condition = "";
				
					condition = SpecialCharFilter.filter(FmtUtils
							.stringForNode(predicate, this.prefixMapping))
							+ " = '" + string + "'";
				
				whereConditions.add(condition);
			} else {
				vars.add(object.getName());
			}
		}

		for (String var : vars) {
			String[] mapsTo = mapping.get(var);
			if (mapsTo.length > 1) {
				select.addSelector(var, new String[] { mapsTo[1] });
			} else {
				select.addSelector(var, new String[] { mapsTo[0] });
			}
		}

		// FROM

			select.setFrom(Tags.HIVE_TABLENAME);

		// WHERE
		for (String where : whereConditions) {
			select.addConjunction(where);
		}

		// cross join is needed
		if (!crossjoin.isEmpty()) {
			select.setName(this.name + "_" + subQueries++);
			ArrayList<SqlStatement> rights = new ArrayList<SqlStatement>();
			List<String> onStrings = new ArrayList<String>();
			Map<String, String[]> newMapping = Schema.shiftToParent(
					this.mapping, select.getName());
			for (Triple triple : crossjoin) {
				TripleGroup group = new TripleGroup(this.name + "_"
						+ subQueries++, this.prefixMapping, true);
				// group.setMapping(mapping);
				group.add(triple);
				rights.add(group.translate());
				onStrings.add(JoinUtil.generateConjunction(JoinUtil
						.getOnConditions(
								Schema.shiftToParent(this.mapping,
										select.getName()),
								Schema.shiftToParent(group.getMappings(),
										group.getName()))));
				newMapping.putAll(Schema.shiftToParent(group.getMappings(),
						group.getName()));

			}

			this.mapping = newMapping;

			Join join = new Join(this.name, select, rights, onStrings,
					JoinType.natural);
			return join;
		}

		return select;
	}

	public int getSharedVars(TripleGroup other) {
		return JoinUtil.getSharedVars(this.mapping, other.getMappings()).size();
	}

	/**
	 * Join with another TripleGroup.
	 */

	public void join(TripleGroup other) {
		for (String entry : other.mapping.keySet()) {
			if (!mapping.containsKey(entry)) {
				mapping.put(entry, other.mapping.get(entry));
			}
		}

	}

	public int searchTripleSamePredicate(int index) {
		for (int i = 0; i < this.triples.size(); i++) {
			if (i != index
					&& triples.get(index).getPredicate()
							.equals(triples.get(i).getPredicate())) {
				return i;
			}
		}

		return -1;
	}

	public boolean searchTripleSamePredicate(Triple triple) {
		for (int i = 0; i < this.triples.size(); i++) {
			if (triples.get(i).getPredicate().equals(triple.getPredicate())) {
				return true;
			}
		}

		return false;
	}

	public Map<String, String[]> getMappings() {
		Map<String, String[]> temp = new HashMap<String, String[]>();
		// Merge both mappings
		temp.putAll(this.mapping);
		temp.putAll(crossJoinMapping);
		return temp;
	}

	// Changes second part of entry leaving everything else in tact.
	public void shiftOrigin(String parent) {
		this.mapping = Schema.shiftOrigin(this.mapping, parent);
	}

	public void setMapping(Map<String, String[]> mapping) {
		this.mapping = mapping;
	}
}
