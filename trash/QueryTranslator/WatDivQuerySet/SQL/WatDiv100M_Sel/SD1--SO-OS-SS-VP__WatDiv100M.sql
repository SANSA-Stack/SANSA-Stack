SELECT tab1.v1 AS v1 , tab0.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v1 , obj AS v2 
	 FROM sorg__trailer$$2$$
	
	) tab1
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM wsdbm__likes$$1$$
	) tab0
 ON(tab1.v1=tab0.v1)


++++++Tables Statistic
sorg__trailer$$2$$	1	SO	sorg__trailer/wsdbm__likes
	VP	<sorg__trailer>	2406
	SO	<sorg__trailer><wsdbm__likes>	2312	0.96
------
wsdbm__likes$$1$$	1	OS	wsdbm__likes/sorg__trailer
	VP	<wsdbm__likes>	1124672
	OS	<wsdbm__likes><sorg__trailer>	5221	0.0
------
