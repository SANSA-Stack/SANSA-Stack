SELECT tab1.v1 AS v1 , tab0.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v1 , obj AS v2 
	 FROM wsdbm__likes$$2$$
	) tab1
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM rev__reviewer$$1$$
	
	) tab0
 ON(tab1.v1=tab0.v1)


++++++Tables Statistic
rev__reviewer$$1$$	1	OS	rev__reviewer/wsdbm__likes
	VP	<rev__reviewer>	15000000
	OS	<rev__reviewer><wsdbm__likes>	3575332	0.24
------
wsdbm__likes$$2$$	1	SO	wsdbm__likes/rev__reviewer
	VP	<wsdbm__likes>	11246476
	SO	<wsdbm__likes><rev__reviewer>	3470802	0.31
------
