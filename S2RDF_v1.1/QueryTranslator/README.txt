DESCRIPTION:
S2RDF_QueryExecutor is a java program, which translates an input SPARQL query to SQL query with table usage instructions (necessary for ExtVP model usage).

S2RDF_QueryExecutor is an Eclipse project. You need to install Eclipse to compile the project. You can also compile the project on your own way. All what you need is runnable jar file, which is than used by /WatDivQuerySet/translateWatDivQueries.py to generate composite query files (containing list of SQL queries) for QueryExecutor.

The scripts ./WatDivQuerySet/SPARQL/selfGenerated/generateWatDivPathQueries.py and ./WatDivQuerySet/SPARQL/WatDivBasic/generateWatDivBasiQueries.py generates Inc, Sel, Mix and WatDiv SPARQL query sets, which can be translated to SQL using translateWatDivQueries.py script. All scripts contains several settings at the top of code corresponding to e.g. ScaleUB, DataBaseName, statistics directory etc.

For further description see comments at the top of Python-scripts.

EXECUTION:
generateWatDivPathQueries.py -i <sourceDir> -o <targetDir> -s <WatDivScale>
generateWatDivBasicQueries.py -i <sourceDir> -o <targetDir> -s <WatDivScale>
translateWatDivQueries.py -s <sparqlDir (conatins sparqlFiles)> -t <sqlDir>
