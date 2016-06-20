SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM wsdbm__hasGenre$$3$$
	 
	 WHERE obj = 'wsdbm:SubGenre50'
	) tab2
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM mo__conductor$$1$$
	
	) tab0
 ON(tab2.v0=tab0.v0)
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM rdf__type$$2$$
	) tab1
 ON(tab0.v0=tab1.v0)


++++++Tables Statistic
rdf__type$$2$$	0	VP	rdf__type/
	VP	<rdf__type>	14800449
------
mo__conductor$$1$$	0	VP	mo__conductor/
	VP	<mo__conductor>	41830
------
wsdbm__hasGenre$$3$$	0	VP	wsdbm__hasGenre/
	VP	<wsdbm__hasGenre>	5936571
------
