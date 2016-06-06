SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab2.v3 AS v3 
 FROM    (SELECT sub AS v0 
	 FROM sorg__nationality$$2$$
	 
	 WHERE obj = 'wsdbm:Country17'
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
	VP	<sorg__nationality>	2000713
	SS	<sorg__nationality><dc__Location>	801012	0.4
	SS	<sorg__nationality><wsdbm__gender>	1200387	0.6
	SS	<sorg__nationality><rdf__type>	2000713	1.0
------
wsdbm__gender$$3$$	2	SS	wsdbm__gender/sorg__nationality
	VP	<wsdbm__gender>	5999423
	SS	<wsdbm__gender><dc__Location>	2399839	0.4
	SS	<wsdbm__gender><sorg__nationality>	1200387	0.2
	SS	<wsdbm__gender><rdf__type>	5999423	1.0
------
dc__Location$$1$$	1	SS	dc__Location/sorg__nationality
	VP	<dc__Location>	4000049
	SS	<dc__Location><sorg__nationality>	801012	0.2
	SS	<dc__Location><wsdbm__gender>	2399839	0.6
	SS	<dc__Location><rdf__type>	4000049	1.0
------
rdf__type$$4$$	2	SS	rdf__type/sorg__nationality
	VP	<rdf__type>	14800449
	SS	<rdf__type><dc__Location>	4920557	0.33
	SS	<rdf__type><sorg__nationality>	2460595	0.17
	SS	<rdf__type><wsdbm__gender>	7379621	0.5
------
