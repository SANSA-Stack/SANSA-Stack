SELECT tab0.v1 AS v1 , tab0.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 , sub AS v0 
	 FROM sorg__author$$1$$
	) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM wsdbm__likes$$2$$
	) tab1
 ON(tab0.v1=tab1.v1)


++++++Tables Statistic
wsdbm__likes$$2$$	0	VP	wsdbm__likes/
	VP	<wsdbm__likes>	11246476
------
sorg__author$$1$$	0	VP	sorg__author/
	VP	<sorg__author>	399974
------
