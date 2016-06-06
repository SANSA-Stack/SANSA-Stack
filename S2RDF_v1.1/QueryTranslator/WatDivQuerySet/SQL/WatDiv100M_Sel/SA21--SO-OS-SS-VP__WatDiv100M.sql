SELECT tab1.v1 AS v1 , tab0.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v1 , obj AS v2 
	 FROM sorg__email$$2$$
	) tab1
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM rev__reviewer$$1$$
	
	) tab0
 ON(tab1.v1=tab0.v1)


++++++Tables Statistic
rev__reviewer$$1$$	1	OS	rev__reviewer/sorg__email
	VP	<rev__reviewer>	1500000
	OS	<rev__reviewer><sorg__email>	1350581	0.9
------
sorg__email$$2$$	1	SO	sorg__email/rev__reviewer
	VP	<sorg__email>	909754
	SO	<sorg__email><rev__reviewer>	278231	0.31
------
