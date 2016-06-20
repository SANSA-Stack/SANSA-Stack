package queryTranslator;

import java.util.HashMap;
import java.util.Set;

/**
 * 
 * @author ALKA2008
 */
public final class Tags {

	// Global Constants
	public static final String QUERY_PREFIX = "";
	public static final String HIVE_TABLENAME_TRIPLESTORE = "triplestore_parquet";
	public static final String HIVE_BGP = "SqlBGP";
	public static final String HIVE_FILTER = "SqlFilter";
	public static final String HIVE_JOIN = "SqlJoin";
	public static final String HIVE_SEQUENCE = "SqlSequenceJoin";
	public static final String HIVE_LEFTJOIN = "SqlLeftJoin";
	public static final String HIVE_CONDITIONAL = "SqlConditional";
	public static final String HIVE_UNION = "SqlUnion";
	public static final String HIVE_PROJECT = "SqlProject";
	public static final String HIVE_DISTINCT = "SqlDistinct";
	public static final String HIVE_ORDER = "SqlOrder";
	public static final String HIVE_SLICE = "SqlSlice";
	public static final String HIVE_REDUCED = "SqlReduced";

	// spark constants
	public static int PLACE_HOLDER_ID = 0;
	public static float ScaleUB = 1f;
	public static Boolean ALLOW_SO= false;
	public static Boolean ALLOW_OS= false;
	public static Boolean ALLOW_SS= false;
	public static String SPARK_STATISTIC_DIRECTORY  = "data";
	public static String VP_TABLE_STAT= SPARK_STATISTIC_DIRECTORY + "/stat_vp.txt";
	public static String SO_TABLE_STAT= SPARK_STATISTIC_DIRECTORY + "/stat_so.txt";
	public static String OS_TABLE_STAT= SPARK_STATISTIC_DIRECTORY + "/stat_os.txt";
	public static String SS_TABLE_STAT= SPARK_STATISTIC_DIRECTORY + "/stat_ss.txt";
	
	public static final String BGP = "tab";
	public static final String PPBGP = "PPtab";
	public static final String FILTER = "FILTER";
	public static final String JOIN = "JOIN";
	public static final String SEQUENCE = "SEQUENCE_JOIN";
	public static final String LEFT_JOIN = "OPTIONAL";
	public static final String CONDITIONAL = "OPTIONAL";
	public static final String UNION = "UNION";
	public static final String PROJECT = "PROJECTION ";
	public static final String DISTINCT = "SM_Distinct";
	public static final String ORDER = "SM_Order";
	public static final String SLICE = "SM_Slice";
	public static final String REDUCED = "SM_Reduced";

	public static final String GREATER_THAN = " > ";
	public static final String GREATER_THAN_OR_EQUAL = " >= ";
	public static final String LESS_THAN = " < ";
	public static final String LESS_THAN_OR_EQUAL = " <= ";
	public static final String EQUALS = " = ";
	public static final String NOT_EQUALS = " != ";
	public static final String LOGICAL_AND = " AND ";
	public static final String LOGICAL_OR = " OR ";
	public static final String LOGICAL_NOT = "NOT ";
	public static final String BOUND = " is not NULL";
	public static final String NOT_BOUND = " is NULL";

	public static final String NO_VAR = "#noVar";
	public static final String NO_SUPPORT = "#noSupport";

	
	public static final String QUERY_STORED_AS = "textfile";
	//public static final String QUERY_SUFFIX = "\nPROFILE; ";

	public static final String OFFSETCHAR = "\t";
	public static final String SUBJECT_COLUMN_NAME = "sub";
	public static final String PREDICATE_COLUMN_NAME = "pred";
	public static final String OBJECT_COLUMN_NAME = "obj";
	public static final String PREDICATE_SOCCESSORS_COLUMN_NAME = "sPredToObj";

	public static final int LIMIT_LARGE_NUMBER = 100000000;
	public static final String ADD = "+";
	public static final String SUBTRACT = "-";
	public static final String LIKE = " LIKE ";
	public static final String LANG_MATCHES = " LIKE ";

	// Global Fields
	public static String HIVE_TABLENAME = "defTable";
	public static String delimiter = " ";
	public static String defaultReducer = "%default reducerNum '1';";
	public static String udf = "SqlSPARQL_udf.jar";
	public static String indata = "indata";
	public static String rdfLoader = "sqlsparql.rdfLoader.ExNTriplesLoader";
	public static String resultWriter = "SqlStorage";
	public static int globalOffsetTabs = 0;

	public static boolean expandPrefixes = false;
	public static boolean optimizer = true;
	public static boolean joinOptimizer = false;
	public static boolean filterOptimizer = true;
	public static boolean bgpOptimizer = true;
	public static boolean existentialQuery = false;
	public static boolean successorsQuery = false;
	public static boolean loopQuery = false;

	// names which cannot be used for columns and their replacements
	public static HashMap<String, String> restrictedNames = new HashMap<String, String>();
	static {
		restrictedNames.put("comment", "comme");
		restrictedNames.put("date", "dat");
		restrictedNames.put("?comment", "comme");
		restrictedNames.put("?date", "dat");
	}

	// Suppress default constructor for noninstantiability
	private Tags() {
	}

}
