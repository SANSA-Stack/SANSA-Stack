# ReadMe

DistAD: A Distributed Generic Anomaly Detection Framework over Large KGs

This package provides a framework for anomaly detection on huge Knowledge Graphs.

This module is a generic, scalable, and distributed framework for anomaly detection on large RDF knowledge graphs. DistAD provides a great granularity for the end-users to select from a vast number of different algorithms, methods, and (hyper-)parameters to detect outliers.
The framework performs anomaly detection by extracting semantic features from entities for calculating similarity, applying clustering on the entities, and running multiple anomaly detection algorithms to detect the outliers on the different levels and granularity. The output of DistAD will be the list of anomalous RDF triples. DistAD needs a config file which looks like as follows:

```groovy
verbose=true
writeResultToFile=false

inputData="dbpediaInfoBox.nt"
resultFilePath="output.nt"

anomalyDetectionType="NumericLiteral" //Possible values: NumericLiteral, Predicate, MultiFeature, CONOD
clusteringMethod="BisectingKmeans" //Possible values: BisectingKmeans, MinHashLSH
clusteringType="Full" //Possible values: Full, Partial
anomalyDetectionAlgorithm="IQR" //Possible values: IQR, MAD, ZSCORE
featureExtractor="Pivot" //Possible values:Pivot, Literal2Feature

anomalyListSize = 10
numberOfClusters = 2
silhouetteMethod = false
silhouetteMethodSamplingRate = 0.1

//CONOD
pairWiseDistanceThreshold = 0.43
  
//IsolationForest
maxSampleForIF = 4
numEstimatorsForIF = 10

//Literal2Feature
depth=1
seedNumber=20
```

After providing a config file, use can easily start the anomaly detection process. The process will read the config file nad based on the values will initiate the corresponding classes.
```scala
val configFilePath = "/PATH/TO/config.conf"
AnomalyDetectionDispatcher.main(Array(configFilePath))
```

In the following each parameter in the `config` file has been explained:

#### verbose 
It is used to print out more information to the user. Possible values are `true` and `false`.
#### writeResultToFile
It indicates if the output should be written to a file. Possible values are `true` and `false`. In case it is set to `true`, then `resultFilePath` should be set as well properly.
#### inputData
The input file path. Is it a `String` value and can refer to local file system or HDFS.
#### resultFilePath
The output file path which can refer to local file system or HDFS.
#### anomalyDetectionType
This `String` value decides on which level the anomaly detection should be applied. The possible values are `NumericLiteral`, `Predicate`, and `MultiFeature`. 

In the `NumericLiteral` mode, all the anomalies have been detected on the level of numeric literals.  For example, if the height of a person is "5" meter, this value should be detected as an anomaly.

In the `Predicate` mode, all the anomalies have been detected on the predicate level. This type of error happens when an entity has more/less than a usual number of the same predicate. For example, a person normally may have none to a few children. However, if he/she has, for example, 200 children, then this type of (potential) error should be detected.

In the `MultiFeature` mode, the algorithm considers the correlation between different features to detect the anomalies. For example, there is a positive correlation between the height and the age of a person. By considering these features separately, the algorithm will not detect a person with 1.8m height as abnormal. However, having contextual information such as the person's age (e.g. 2 years old) can make this combination anomalous.

In the `CONOD` mode, the CONOD algorithm will be executed for only numerical literals. CONOD is a scalable and generic algorithm for numeric outlier detection in DBpedia. It utilized *rdf:type* and Linked Hypernyms Dataset (LHD) for creating cohorts. Cohorts, unlike clusters, could overlap with each other. For cohorts, CONOD proposed a novel clustering approach based on Locality Sensitive Hashing (LSH). We modified CONOD to be able to run it over any arbitrary dataset not only DBpedia. For more information please check [CONOD](https://hajirajabeen.github.io/publications/CONOD_.pdf).

#### clusteringMethod
Clustering methods, decides how clustering should happen to group the different entities. Clustering is important in anomaly detection over KGs because traditional methods may gather all values of a certain predicate, such as *dbp:weight* and attempt to detect anomalies for this feature. However, in KGs, comparing a feature from different entity types (e.g. the weight of persons against the weight of vehicles) is logically incorrect. The possible values are `BisectingKmeans` and `MinHashLSH`. For more information about the clustering methods please refer to the DistAD paper.

#### clusteringType
This value controls which features should be passed to the similarity calculation module. The possible values are `Full` and `Partial`.  In `Full` similarity we consider all the existing predicates of an entity to calculate the similarity between entities. In `Partial` mode, we only consider predicates that have numeric literals as objects. Although the accuracy of the `Full` mode is higher due to considering all the available predicates, the  `Partial` mode benefits from faster operation due to less number of predicates. 

#### anomalyDetectionAlgorithm
This value decide which anomaly detection algorithm should be applied. The possible values are `IQR`, `ZSCORE`, and `MAD`. These algorithms have been implemeted to be used in the `NumericLiteral` and `Predciate` modes. For the `MultiFeature` mode, `IsolationForest` algorithm will be used automatically. For more information about each algorithm please refer to the DistAD paper.

#### featureExtractor
After clustering entities, the framework needs to extract features to be able to run anomaly detection algorithms over them. The possible values are `PIVOT` and `Literal2Feature`.
Pivoting is a reshape mechanism that Spark provides over dataframes. Pivoting reshapes data (produce a "pivot" table) based on column values. The following example depicts how pivoting works on sample RDF data if one wants to pivot based on "Predicate" and aggregate over "Object":
```
+-----------------+--------------+-----------+
|Subject          |Predicate     |Object     |
+-----------------+--------------+-----------+
|dbr:Barack_Obama |dbo:birthPlace|dbr:Hawaii |
|dbr:Barack_Obama |dbo:birthDate |1961-08-04 |
|dbr:Angela_Merkel|dbo:birthPlace|dbr:Hamburg|
|dbr:Angela_Merkel|dbo:birthDate |1954-07-17 |
+-----------------+--------------+-----------+
```
The result after pivoting will be:
```
+-----------------+-------------+--------------+
|Subject          |dbo:birthDate|dbo:birthPlace|
+-----------------+-------------+--------------+
|dbr:Barack_Obama |1961-08-04   |dbr:Hawaii    |
|dbr:Angela_Merkel|1954-07-17   |dbr:Hamburg   |
+-----------------+-------------+--------------+
```
[Literal2Feature](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml#literal2feature-autosparql-generation-for-feature-extraction) is a generic, distributed, and
a scalable software framework that can automatically transform a given RDF dataset to a standard feature matrix by deep traversing the RDF graph and extracting literals to a given depth. The result of Literal2Feature is a SPARQL query that extracts the features. This option helps the user to extract features that are not in the direct vicinity of an entity for the outlier detection purpose.

#### depth
If `Literal2Feature` is selected, then this value controls how deep the algorithm should go into the KG to extract the features. For more information please check [Literal2Feature paper](https://hajirajabeen.github.io/publications/Literal2Feature_Semantics_2021_CR.pdf)

#### seedNumber
If `Literal2Feature` is selected, then this value controls how many seeds should be selected to extract the features. For more information please check [Literal2Feature paper](https://hajirajabeen.github.io/publications/Literal2Feature_Semantics_2021_CR.pdf) 

#### anomalyListSize
Applying anomaly detection algorithms over a list of numbers which their size is small is not accurate. This value controls the threshold of the size of the list. If the size of the list of numbers is smaller than this value, the anomaly detection won't be run on that list. This value is an `Integer` value.

#### numberOfClusters
Use can set number of preferred clusters. In case `silhouetteMethod` is set to `true`, this value will be ignored.

#### silhouetteMethod
Normally the number of clusters is not clear beforehand. So by setting this value to `true`, the framework tries to detect the best matching cluster number for the input data.

#### silhouetteMethodSamplingRate
In case the `silhouetteMethod` to `true`, this value will control the sampling rate. As the knowledge graphs are huge, trying to detect the best matching cluster number over the whole KG is not feasible. This `Double` value controls the percentage of the data which should be used for the cluster number detection. For example, `0.01` means 1% of data should be sampled randomly.  

#### pairWiseDistanceThreshold
In case `MinHashLSH` has been selected for the clustering algorithm, this value controls the threshold value for the pairwise similarity. All the pairwise similarity under this value will be ignored.

#### maxSampleForIF
In case `MultiFeature` has been selected for the anomaly detection type, then `IsolationForest` algorithm will be used for the anomaly detection over multiple features. This value controls number of samples `IsolationForest` should use.

#### numEstimatorsForIF
In case `MultiFeature` has been selected for the anomaly detection type, then `IsolationForest` algorithm will be used for the anomaly detection over multiple features. This value controls number of estimators `IsolationForest` should use.

