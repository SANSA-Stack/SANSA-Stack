SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab2.v3 AS v3 
 FROM    (SELECT sub AS v0 
	 FROM sorg__nationality$$2$$
	 
	 WHERE obj = 'wsdbm:Country13'
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
	VP	<sorg__nationality>	19924
	SS	<sorg__nationality><dc__Location>	7970	0.4
	SS	<sorg__nationality><wsdbm__gender>	11925	0.6
	SS	<sorg__nationality><rdf__type>	19924	1.0
------
wsdbm__gender$$3$$	2	SS	wsdbm__gender/sorg__nationality
	VP	<wsdbm__gender>	59784
	SS	<wsdbm__gender><dc__Location>	24028	0.4
	SS	<wsdbm__gender><sorg__nationality>	11925	0.2
	SS	<wsdbm__gender><rdf__type>	59784	1.0
------
dc__Location$$1$$	1	SS	dc__Location/sorg__nationality
	VP	<dc__Location>	40297
	SS	<dc__Location><sorg__nationality>	7970	0.2
	SS	<dc__Location><wsdbm__gender>	24028	0.6
	SS	<dc__Location><rdf__type>	40297	1.0
------
rdf__type$$4$$	2	SS	rdf__type/sorg__nationality
	VP	<rdf__type>	136215
	SS	<rdf__type><dc__Location>	44778	0.33
	SS	<rdf__type><sorg__nationality>	22129	0.16
	SS	<rdf__type><wsdbm__gender>	66480	0.49
------
