**# Scalable Numerical Outlier Detection in RDF**  
This repository(anomalydetection) consists of three different implementations for detecting numerical outliers in RDF with 
one main class. These three implementations are different due to the difference in the approaches for cohorting the subjects. 
All the techniques have their pros and cons as discussed below:-  

1. Implementation with dataframe crossJoin function:
   The subjects are cohorted according to the rdf:type and hypernym. The crossJoin function is used to create 
   the pair rdd so that similar subjects can be cohorted. CrossJoin is one of the most time-consuming join and 
   should be avoided. It works well on small datasets(tested on 3.6 GB) but does not work for large datasets(tested on 
   16.6 GB dataset)
   
2. ApproxSimilarityJoin with CountvectorizerModel:-
   Spark inbuilt function ApproxSimilarityJoin is used with CountvectorizerModel to find similarity between 
   subjects. ApproxSimilarityJoin takes input as feature vectors and CountvectorizerModel helps in creating the features from 
   the data. CountvectorizerModel scans data twice-one for building model and another for transformation. It also needs extra 
   space equal to the number of unique features. ApproxSimilarityJoin with CountvectorizerModel performs better than 
   crossJoin on a big dataset (16.6 GB).
   
3. ApproxSimilarityJoin with HashigTF:-
   ApproxSimilarityJoin with HashigTF is another way to calculate the similarity between the subjects. HashigTF also 
   helps in creating the features from the data like CountvectorizerModel. It scans the data only once and does not require 
   any additional storage. ApproxSimilarityJoin with HashigTF performs better than the techniques mentioned above on big 
   dataset(tested on 16.6 GB dataset).  
  
 **Framework and Tools used:-**
   * Spark framework
   * Scala programming language
   * Hadoop file system(HDFS) for storing the data
   * Maven
  
  # **Command to run the application**:  

sudo ./bin/spark-submit --class net.sansa_stack.template.spark.AnomalyDetection.Main --driver-memory 50G --executor-memory 200G --master spark://172.18.160.16:3077 /data/home/RajatDadwal/app-jar/SANSA-OutLierDetection-Maven-Spark-0.3.1-SNAPSHOT-jar-with-dependencies.jar --input hdfs://172.18.160.17:54310/RajatDadwal/input/recent_dataset/Dbpedia.nt --threshold 0.45 --anomalyListLimit 10 --numofpartition 125 --output hdfs://172.18.160.17:54310/RajatDadwal/output/resultDbpediaLib --optionChange 1
 
 The above parameters are explained as follows:-
 *  net.sansa_stack.template.spark.AnomalyDetection.Main -The entry point for your application  
    (e.g. net.sansa_stack.template.spark.AnomalyDetection.Main)
 * --driver-memory- Amount of memory to use for the driver process, i.e. where SparkContext is initialized. (e.g. 
    1g 2g). 
 * --executor-memory -Amount of memory to use per executor process (e.g. 2g, 8g).
 *  --master  -The master URL for the cluster(spark://172.18.160.16:3077) 
 *  app-jar- Path to a bundled jar including your application and all dependencies(/data/home/RajatDadwal/app-
    jar/SANSA-OutLierDetection-Maven-Spark-0.3.1-SNAPSHOT-jar-with-dependencies.jar)
 * --input - input dataset store in the hdfs ( 
       hdfs://172.18.160.17:54310/RajatDadwal/input/recent_dataset/Dbpedia.nt)
 *  --threshold - jaccard distance threshole value i.e 0.45. 
 *  --anomalyListLimit- size of list containing numerical triples for anomaly detection. We prune subpopulations which contain a low 
                        number of instances or maybe no instances at all. The default value is 10.(minimum 10 instances should be there 
                        for the calculation of IQR)
 *  --numofpartition- number of partition 
 * --output - output file path
 * --optionChange - option for selecting different methods like CroosJoin,ApproxSimilarityJoin etc.
 
 
 
  
