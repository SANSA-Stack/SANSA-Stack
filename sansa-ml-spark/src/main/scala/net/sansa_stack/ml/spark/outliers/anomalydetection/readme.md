**# Scalable Numerical Outlier Detection in RDF**  
This repository consists of three different implementations for detecting outliers in RDF. All the techniques have their pros and cons as discussed below:-
1. Dataframe CroosJoin implementation:
   The subjects are cohorted according to the rdf:type and hypernym. The cross join function is used to create the pair rdd for 
   finding similarity between subjects. Crossjoin is one of the most time-consuming joins and often should be avoided. It works 
   well for small datasets(tested on 3.6 GB) but fails for large datasets(tested on 
   16.6 GB dataset)
   
2. ApproxSimilarityJoin with CountvectorizerModel:-
   Spark inbuilt function ApproxSimilarityJoin is used with CountvectorizerModel to find similarity between subjects.
   CountvectorizerModel helps in creating the features from the data. CountvectorizerModel Scans data twice-one for building 
   model and another for transformation. It performs better than crossJoin on a big dataset (16.6 GB).
   
3. ApproxSimilarityJoin with HashigTF:-
   ApproxSimilarityJoin is with HashigTF is used to find similarity between the subjects. HashigTF also helps in creating the 
   features from the data.HashigTF scans the data only once and hence performs better than the technique mentioned above on big 
   dataset(tested on 16.6 GB dataset). 
