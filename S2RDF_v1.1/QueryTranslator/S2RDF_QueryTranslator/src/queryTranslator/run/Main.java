package queryTranslator.run;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

import queryTranslator.SparkTableStatistics;
import queryTranslator.Tags;
import queryTranslator.Translator;


/**
 * Main Class for program start. Parses the commandline arguments and calls the
 * translator.
 * 
 * Options: 
 * -h, --help prints the usage help message 
 * -d, --delimiter <value> delimiter used in RDF triples if not whitespace 
 * -e, --expand expand prefixes used in the query 
 * -succ, --successors use tables with successors, 
 * -loop, --void_loops avoid loops in result set 
 * -exi, --existential valuation of property path as extensibility query
 * -table, --table_name <name> set name of the HIVE table
 * -i, --input <file> SPARQL query file to translate 
 * -o, --output <file> Sql
 *  output script file
 * 
 * @author Simon Skilevic
 */
public class Main {

	private static String inputFile;
	private static String outputFile;
	private static String delimiter = " ";
	private static boolean optimize = false;
	private static boolean expand = false;
	private static String folderName = "";

	// Define a static logger variable so that it references the corresponding
	// Logger instance
	private static final Logger logger = Logger.getLogger(Main.class);

	public static void main(String[] args) {
		// parse the commandline arguments
		parseInput(args);		
		if(!folderName.equals("")){
			File folderfile = new File(folderName);
			for(final File fileEntry : folderfile.listFiles()){
				if(fileEntry.getName().contains("sparql") && !fileEntry.getName().contains("log") && !fileEntry.getName().contains("sql")){
					System.out.println("Tranlsating file "+fileEntry.getName());
					// instantiate Translator
					Translator translator = new Translator(fileEntry.getAbsolutePath(), fileEntry.getAbsolutePath());
					translator.setOptimizer(optimize);
					translator.setExpandMode(expand);
					translator.setDelimiter(delimiter);
					translator.translateQuery();
				}
			}
			
		} else {
		// instantiate Translator
		Translator translator = new Translator(inputFile, outputFile);
		translator.setOptimizer(optimize);
		translator.setExpandMode(expand);
		translator.setDelimiter(delimiter);
		translator.translateQuery();
		}
	}

	/**
	 * Parses the commandline arguments.
	 * 
	 * @param args
	 *            commandline arguments
	 */
	@SuppressWarnings("static-access")
	private static void parseInput(String[] args) {
		// DEFINITION STAGE
		Options options = new Options();
		Option help = new Option("h", "help", false, "print this message");
		options.addOption(help);
		/*Option optimizer = new Option("opt", "optimize", false,
				"turn on SPARQL algebra optimization");
		options.addOption(optimizer);*/
		Option prefixes = new Option("e", "expand", false,
				"expand URI prefixes");
		Option soTables = new Option("so", "so_tables", false,
				"use SO Tables");
		Option osTables = new Option("os", "os_tables", false,
				"use OS Tables");
		Option ssTables = new Option("ss", "ss_tables", false,
				"use SS Tables");
		options.addOption(prefixes);
		options.addOption(osTables);
		options.addOption(soTables);
		options.addOption(ssTables);
		Option delimit = OptionBuilder
				.withArgName("value")
				.hasArg()
				.withDescription(
						"delimiter used in RDF triples if not whitespace")
				.withLongOpt("delimiter").isRequired(false).create("d");
		options.addOption(delimit);
		Option sparkStatisticDir = OptionBuilder.withArgName("spark_statistic_dir").hasArg()
				.withDescription("Directory for the ExtVP-tables-statistic-files")
				.withLongOpt("spark_statistic_dir").isRequired(true).create("sd");
		options.addOption(sparkStatisticDir);
		Option scaleUB = OptionBuilder.withArgName("scaleUB").hasArg()
				.withDescription("Upper Bound for ExtVP tables scale (def=1)")
				.withLongOpt("scaleUB").isRequired(false).create("sUB");
		options.addOption(scaleUB);
		Option input = OptionBuilder.withArgName("file").hasArg()
				.withDescription("SPARQL query file to translate")
				.withLongOpt("input").isRequired(true).create("i");
		options.addOption(input);
		Option output = OptionBuilder.withArgName("file").hasArg()
				.withDescription("SQL utput script file")
				.withLongOpt("output").isRequired(false).create

				("o");
		options.addOption(output);

		Option folder = OptionBuilder.withArgName("folder").hasArg()
				.withDescription("SQL output script file")
				.withLongOpt("folder").isRequired(false).create("f");
		options.addOption(folder);

		// PARSING STAGE
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			// parse the command line arguments
			cmd = parser.parse(options, args);
		} catch (ParseException exp) {
			// error when parsing commandline arguments
			// automatically generate the help statement
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("SparqlEvaluator", options, true);
			logger.fatal(exp.getMessage(), exp);
			System.exit(-1);
		}

		// INTERROGATION STAGE
		if (cmd.hasOption("help")) {
			// automatically generate the help statement
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("SparqlEvaluator", options, true);
		}
		if (cmd.hasOption("optimize")) {
			optimize = true;
			logger.info("SPARQL Algebra optimization is turned on");
		}
		if (cmd.hasOption("expand")) {
			expand = true;
			logger.info("URI prefix expansion is turned on");
		}
		if (cmd.hasOption("delimiter")) {
			delimiter = cmd.getOptionValue("delimiter");
			logger.info("Delimiter for RDF triples: " + delimiter);
		}
		if (cmd.hasOption("input")) {
			inputFile = cmd.getOptionValue("input");
		}
		
		if (cmd.hasOption("output")) {
			outputFile = cmd.getOptionValue("output");
		} else {
			outputFile = cmd.getOptionValue("input");
		}

		if (cmd.hasOption("folder")) {
			folderName = cmd.getOptionValue("folder");
		}
		
		if (cmd.hasOption("so_tables")) {
			Tags.ALLOW_SO = true;
		}
		if (cmd.hasOption("os_tables")) {
			Tags.ALLOW_OS = true;
		}
		if (cmd.hasOption("ss_tables")) {
			Tags.ALLOW_SS =true;
		}
		if (cmd.hasOption("spark_statistic_dir")) {
			Tags.SPARK_STATISTIC_DIRECTORY = cmd.getOptionValue("spark_statistic_dir");
			Tags.VP_TABLE_STAT= Tags.SPARK_STATISTIC_DIRECTORY + "/stat_vp.txt";
			Tags.SO_TABLE_STAT= Tags.SPARK_STATISTIC_DIRECTORY + "/stat_so.txt";
			Tags.OS_TABLE_STAT= Tags.SPARK_STATISTIC_DIRECTORY + "/stat_os.txt";
			Tags.SS_TABLE_STAT= Tags.SPARK_STATISTIC_DIRECTORY + "/stat_ss.txt";
		}
		if (cmd.hasOption("scaleUB")) {
			Tags.ScaleUB = Float.valueOf(cmd.getOptionValue("scaleUB"));
		}
	}

}
