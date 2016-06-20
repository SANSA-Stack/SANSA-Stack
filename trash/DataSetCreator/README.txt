DESCRIPTION:
DataSetCreator generates ExtVP model in HDFS for a given RDF dataset.
The input RDF dataset has to be saved in tsv file (tab separated) with three columns (subject, predicate, object) and placed in HDFS in directory, where you want to create the database. During the creation process DataSetCreator generates statistic files (stat_os.txt, stat_so.txt, stat_ss.txt, stat_vt.txt), which are used for SPARQL query translation to SQL by QueryTranslator.

S2RDF DataSetCreator is an sbt-scala project. You need to install sbt to compile the project. You can also compile the project on your own way. All what you need is runnable jar file, which is than committed to Spark-Cluster using DataSetCreator.py script. The script contains also several settings to deploy Spark-claster and S2RDF_DataSetCreator.

For further description see comments at the top of Python-scripts.

INSTALLATION:
cd S2RDF_DataSetCreator
sbt package

EXECUTION:
python DataSetCreator.py -i <inputRDFFile> [-s <ScaleUB> (def=1)]
