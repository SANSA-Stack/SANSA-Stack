SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v0 
	 FROM wsdbm__likes$$3$$ 
	 WHERE sub = 'wsdbm:User2329730'
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
wsdbm__likes$$3$$	2	OS	wsdbm__likes/sorg__text
	VP	<wsdbm__likes>	11246476
	OS	<wsdbm__likes><rdf__type>	11246476	1.0
	OS	<wsdbm__likes><sorg__text>	3276540	0.29
------
rdf__type$$1$$	1	SS	rdf__type/sorg__text
	VP	<rdf__type>	14800449
	SS	<rdf__type><sorg__text>	749948	0.05
	SO	<rdf__type><wsdbm__likes>	2362781	0.16
------
sorg__text$$2$$	2	SO	sorg__text/wsdbm__likes
	VP	<sorg__text>	749948
	SS	<sorg__text><rdf__type>	749948	1.0
	SO	<sorg__text><wsdbm__likes>	708787	0.95
------
