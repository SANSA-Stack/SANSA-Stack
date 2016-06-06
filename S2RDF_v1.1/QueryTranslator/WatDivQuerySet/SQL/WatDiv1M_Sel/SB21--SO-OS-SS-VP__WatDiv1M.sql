SELECT tab1.v1 AS v1 , tab0.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v1 , obj AS v2 
	 FROM wsdbm__likes$$2$$
	) tab1
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM wsdbm__follows$$1$$
	
	) tab0
 ON(tab1.v1=tab0.v1)


++++++Tables Statistic
wsdbm__likes$$2$$	1	SO	wsdbm__likes/wsdbm__follows
	VP	<wsdbm__likes>	11256
	SO	<wsdbm__likes><wsdbm__follows>	10495	0.93
------
wsdbm__follows$$1$$	1	OS	wsdbm__follows/wsdbm__likes
	VP	<wsdbm__follows>	330403
	OS	<wsdbm__follows><wsdbm__likes>	78618	0.24
------
