**# Scalable Numerical Outlier Detection in RDF**  
This repository consists of three different implementations for detecting outliers in RDF. All the techniques have their pros and cons as discussed below:-  

1. Dataframe CroosJoin implementation:
   The subjects are cohorted according to the rdf:type and hypernym. The cross join function is used to create the pair rdd for 
   finding similarity between subjects. Crossjoin is one of the most time-consuming joins and often should be avoided. It works 
   well for small datasets(tested on 3.6 GB) but fails for large datasets(tested on 16.6 GB dataset)
   
2. ApproxSimilarityJoin with CountvectorizerModel:-
   Spark inbuilt function ApproxSimilarityJoin is used with CountvectorizerModel to find similarity between subjects.
   CountvectorizerModel helps in creating the features from the data. CountvectorizerModel Scans data twice-one for building 
   model and another for transformation. It performs better than crossJoin on a big dataset (16.6 GB).
   
3. ApproxSimilarityJoin with HashigTF:-
   ApproxSimilarityJoin is with HashigTF is used to find similarity between the subjects. HashigTF also helps in creating the 
   features from the data.HashigTF scans the data only once and hence performs better than the technique mentioned above on big 
   dataset(tested on 16.6 GB dataset). 
   
  **#Command to run the application  
  
  Framework and Tools used:-
  1. Spark framework
  2  Scala programming language
  3  Hadoop file system(HDFS) for stroing the data
  4  Maven
  
  Command to run the application:
sudo ./bin/spark-submit --class net.sansa_stack.template.spark.AnomalyDetection.Main --driver-memory 50G --executor-memory 200G --master spark://172.18.160.16:3077 /data/home/RajatDadwal/app-jar/SANSA-OutLierDetection-Maven-Spark-0.3.1-SNAPSHOT-jar-with-dependencies.jar --input hdfs://172.18.160.17:54310/RajatDadwal/input/recent_dataset/Dbpedia.nt --threshold 0.45 --anomalyListLimit 10 --numofpartition 125 --output hdfs://172.18.160.17:54310/RajatDadwal/output/resultDbpediaLib --optionChange 1
 
 The above parameters are explained as follows:-
 1) net.sansa_stack.template.spark.AnomalyDetection.Main -The entry point for your application 
   (e.g.org.apache.spark.examples.SparkPi)
 2) --driver-memory 50G- Amount of memory to use for the driver process, i.e. where SparkContext is initialized. (e.g. 1g, 
     2g). 
 3) --executor-memory -Amount of memory to use per executor process (e.g. 2g, 8g).
 4) --master spark://172.18.160.16:3077 -The master URL for the cluster 
 5) /data/home/RajatDadwal/app-jar/SANSA-OutLierDetection-Maven-Spark-0.3.1-SNAPSHOT-jar-with-dependencies.jar- Path to a 
     bundled jar including your application and all dependencies
 6) --input hdfs://172.18.160.17:54310/RajatDadwal/input/recent_dataset/Dbpedia.nt - input dataset store in the hdfs
 7) --threshold 0.45 -- jaccard distance threshole value. 
 
 
  
