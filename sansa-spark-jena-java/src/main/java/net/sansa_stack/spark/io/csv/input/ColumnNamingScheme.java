package net.sansa_stack.spark.io.csv.input;

import com.google.common.base.Preconditions;
import org.aksw.commons.model.csvw.domain.impl.CsvwLib;
import org.aksw.jenax.arq.util.var.VarUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

public interface ColumnNamingScheme {
	public String[][] create(String[] row);

	public static String[][] row(String[] row) {
		int n = row.length;
		String[][] r = new String[n][];
		for (int i = 0; i < n; ++i) {
			String v = row[i];
			r[i] = v == null ? new String[0] : new String[] { v };
		}
		return r;
	}

	/**
	 * Convert the column headings such that they are safe for use with sparql variables.
	 * Replaces all non-sparql-variable characters with underscore.
	 * */
	public static String[][] sparql(String[] row) {
		int n = row.length;
		String[][] r = new String[n][];
		for (int i = 0; i < n; ++i) {
			String v = row[i];
			r[i] = v == null ? new String[0] : new String[] { VarUtils.safeVarName(v) };
		}
		return r;
	}

	public static String[][] excel(int length) {
		String[][] r = new String[length][];
		for (int i = 0; i < length; ++i) {
			r[i] = new String[] { CsvwLib.getExcelColumnLabel(i) };
		}
		return r;
	}

	public static String[][] number(int offset, int length) {
		String[][] r = new String[length][];
		for (int i = 0; i < length; ++i) {
			r[i] = new String[] { Integer.toString(offset + i) };
		}
		return r;
	}

	/**
	 * Merges multiple namings such that always only the first one is retained.
	 * 
	 * @param elements
	 * @return
	 */
	public static String[][] merge(Collection<String[][]> elements) {
		Set<Integer> lengths = elements.stream().map(e -> e.length).collect(Collectors.toSet());
		Preconditions.checkArgument(lengths.size() == 1, "Need exactly one length for the columns; got lengths" + lengths);		
		
		int n = lengths.iterator().next();
		
		Set<String> seen = new HashSet<>();

		
		@SuppressWarnings("unchecked")
		List<String>[] cols = new List[n];
		for (int i = 0; i < n; ++i) {
			cols[i] = new ArrayList<>();
		}
		
		for (String[][] element : elements) {
			for (int i = 0; i < n; ++i) {
				List<String> alts = cols[i];
				String[] names = element[i];
				int m = names.length;
				for (int j = 0; j < m; ++j) {
					String name = names[j];
					if (!seen.contains(name)) {
						alts.add(name);
					}
					seen.add(name);
				}
			}
		}
		
		String[][] result = new String[n][];
		for (int i = 0; i < n; ++i) {
			result[i] = cols[i].toArray(new String[0]);
		}
		return result;
	}
	
	public static String[][] createColumnHeadings(List<String> schemeNames, String[] row, boolean rowAsExcel) {
		int n = row.length;
		List<String[][]> elements = new ArrayList<>();
		for (String schemeName : schemeNames) {
			String[][] outNames;
			if (StringUtils.isNumeric(schemeName)) {
				int offset = Integer.parseInt(schemeName);
				outNames = number(offset, n);
			} else if (schemeName.equalsIgnoreCase("excel")) {
				outNames = excel(n);
			} else if (schemeName.equalsIgnoreCase("row")) {
				outNames = rowAsExcel ? excel(row.length) : row(row);
			} else if (schemeName.equalsIgnoreCase("sparql")) {
				outNames = sparql(row);
			} else {
				throw new RuntimeException("Unknown naming scheme: " + schemeNames);
			}
			elements.add(outNames);				
		}
		String[][] result = merge(elements);
		return result;
	}
	
	
	
}
