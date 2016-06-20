SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v0 
	 FROM wsdbm__likes$$3$$ 
	 WHERE sub = 'wsdbm:User10095'
	) tab2
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM sorg__text$$2$$
	) tab1
 ON(tab2.v0=tab1.v0)
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM rdf__type$$1$$
	) tab0
 ON(tab1.v0=tab0.v0)


++++++Tables Statistic
wsdbm__likes$$3$$	0	VP	wsdbm__likes/
	VP	<wsdbm__likes>	112401
------
rdf__type$$1$$	0	VP	rdf__type/
	VP	<rdf__type>	136215
------
sorg__text$$2$$	0	VP	sorg__text/
	VP	<sorg__text>	7476
------
