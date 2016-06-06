SELECT tab1.v1 AS v1 , tab0.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v1 , obj AS v2 
	 FROM sorg__trailer$$2$$
	
	) tab1
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM wsdbm__likes$$1$$
	) tab0
 ON(tab1.v1=tab0.v1)


++++++Tables Statistic
sorg__trailer$$2$$	0	VP	sorg__trailer/
	VP	<sorg__trailer>	17
------
wsdbm__likes$$1$$	0	VP	wsdbm__likes/
	VP	<wsdbm__likes>	11256
------
