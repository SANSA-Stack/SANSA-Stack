SELECT tab0.v0 AS v0 , tab1.v3 AS v3 , tab2.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM wsdbm__subscribes$$1$$
	 
	 WHERE obj = 'wsdbm:Website1106'
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
wsdbm__likes$$3$$	0	VP	wsdbm__likes/
	VP	<wsdbm__likes>	112401
------
wsdbm__subscribes$$1$$	0	VP	wsdbm__subscribes/
	VP	<wsdbm__subscribes>	152275
------
sorg__caption$$2$$	0	VP	sorg__caption/
	VP	<sorg__caption>	2501
------
