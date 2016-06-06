SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM wsdbm__hasGenre$$3$$
	 
	 WHERE obj = 'wsdbm:SubGenre72'
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
rdf__type$$2$$	1	SS	rdf__type/mo__conductor
	VP	<rdf__type>	14856
	SS	<rdf__type><mo__conductor>	44	0.0
	SS	<rdf__type><wsdbm__hasGenre>	2500	0.17
------
mo__conductor$$1$$	0	VP	mo__conductor/
	VP	<mo__conductor>	44
	SS	<mo__conductor><rdf__type>	44	1.0
	SS	<mo__conductor><wsdbm__hasGenre>	44	1.0
------
wsdbm__hasGenre$$3$$	1	SS	wsdbm__hasGenre/mo__conductor
	VP	<wsdbm__hasGenre>	5961
	SS	<wsdbm__hasGenre><mo__conductor>	107	0.02
	SS	<wsdbm__hasGenre><rdf__type>	5961	1.0
------
