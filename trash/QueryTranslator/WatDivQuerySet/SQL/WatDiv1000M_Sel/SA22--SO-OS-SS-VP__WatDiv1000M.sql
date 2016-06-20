SELECT tab1.v1 AS v1 , tab0.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v1 , obj AS v2 
	 FROM foaf__age$$2$$
	) tab1
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM rev__reviewer$$1$$
	
	) tab0
 ON(tab1.v1=tab0.v1)


++++++Tables Statistic
rev__reviewer$$1$$	1	OS	rev__reviewer/foaf__age
	VP	<rev__reviewer>	15000000
	OS	<rev__reviewer><foaf__age>	7496177	0.5
------
foaf__age$$2$$	1	SO	foaf__age/rev__reviewer
	VP	<foaf__age>	4998434
	SO	<foaf__age><rev__reviewer>	1544195	0.31
------
