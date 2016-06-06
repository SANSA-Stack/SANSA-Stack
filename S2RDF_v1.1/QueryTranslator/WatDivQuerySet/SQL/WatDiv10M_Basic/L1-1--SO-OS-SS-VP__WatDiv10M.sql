SELECT tab0.v0 AS v0 , tab1.v3 AS v3 , tab2.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM wsdbm__subscribes$$1$$
	 
	 WHERE obj = 'wsdbm:Website2192'
	) tab0
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM wsdbm__likes$$3$$
	) tab2
 ON(tab0.v0=tab2.v0)
 JOIN    (SELECT obj AS v3 , sub AS v2 
	 FROM sorg__caption$$2$$
	
	) tab1
 ON(tab2.v2=tab1.v2)


++++++Tables Statistic
wsdbm__likes$$3$$	2	OS	wsdbm__likes/sorg__caption
	VP	<wsdbm__likes>	112401
	SS	<wsdbm__likes><wsdbm__subscribes>	22607	0.2
	OS	<wsdbm__likes><sorg__caption>	9466	0.08
------
wsdbm__subscribes$$1$$	1	SS	wsdbm__subscribes/wsdbm__likes
	VP	<wsdbm__subscribes>	152275
	SS	<wsdbm__subscribes><wsdbm__likes>	36410	0.24
------
sorg__caption$$2$$	1	SO	sorg__caption/wsdbm__likes
	VP	<sorg__caption>	2501
	SO	<sorg__caption><wsdbm__likes>	2371	0.95
------
