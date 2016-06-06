SELECT tab1.v1 AS v1 , tab0.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v1 , obj AS v2 
	 FROM sorg__jobTitle$$2$$
	
	) tab1
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM rev__reviewer$$1$$
	
	) tab0
 ON(tab1.v1=tab0.v1)


++++++Tables Statistic
rev__reviewer$$1$$	1	OS	rev__reviewer/sorg__jobTitle
	VP	<rev__reviewer>	150000
	OS	<rev__reviewer><sorg__jobTitle>	7535	0.05
------
sorg__jobTitle$$2$$	1	SO	sorg__jobTitle/rev__reviewer
	VP	<sorg__jobTitle>	5008
	SO	<sorg__jobTitle><rev__reviewer>	1562	0.31
------
