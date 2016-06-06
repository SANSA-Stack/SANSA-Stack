SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab2.v3 AS v3 
 FROM    (SELECT sub AS v0 
	 FROM sorg__nationality$$2$$
	 
	 WHERE obj = 'wsdbm:Country12'
	) tab1
 JOIN    (SELECT sub AS v0 
	 FROM rdf__type$$4$$ 
	 WHERE obj = 'wsdbm:Role2'
	) tab3
 ON(tab1.v0=tab3.v0)
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM dc__Location$$1$$
	) tab0
 ON(tab3.v0=tab0.v0)
 JOIN    (SELECT sub AS v0 , obj AS v3 
	 FROM wsdbm__gender$$3$$
	
	) tab2
 ON(tab0.v0=tab2.v0)


++++++Tables Statistic
sorg__nationality$$2$$	1	SS	sorg__nationality/dc__Location
	VP	<sorg__nationality>	200780
	SS	<sorg__nationality><dc__Location>	80220	0.4
	SS	<sorg__nationality><wsdbm__gender>	120546	0.6
	SS	<sorg__nationality><rdf__type>	200780	1.0
------
wsdbm__gender$$3$$	2	SS	wsdbm__gender/sorg__nationality
	VP	<wsdbm__gender>	600332
	SS	<wsdbm__gender><dc__Location>	240211	0.4
	SS	<wsdbm__gender><sorg__nationality>	120546	0.2
	SS	<wsdbm__gender><rdf__type>	600332	1.0
------
dc__Location$$1$$	1	SS	dc__Location/sorg__nationality
	VP	<dc__Location>	400344
	SS	<dc__Location><sorg__nationality>	80220	0.2
	SS	<dc__Location><wsdbm__gender>	240211	0.6
	SS	<dc__Location><rdf__type>	400344	1.0
------
rdf__type$$4$$	2	SS	rdf__type/sorg__nationality
	VP	<rdf__type>	1480374
	SS	<rdf__type><dc__Location>	492793	0.33
	SS	<rdf__type><sorg__nationality>	247127	0.17
	SS	<rdf__type><wsdbm__gender>	738943	0.5
------
