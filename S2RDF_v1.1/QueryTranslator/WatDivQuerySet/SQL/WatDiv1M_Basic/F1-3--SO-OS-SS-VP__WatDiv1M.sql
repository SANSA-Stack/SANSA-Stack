SELECT tab4.v0 AS v0 , tab3.v5 AS v5 , tab2.v4 AS v4 , tab5.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v3 
	 FROM rdf__type$$6$$ 
	 WHERE obj = 'wsdbm:ProductCategory2'
	) tab5
 JOIN    (SELECT obj AS v5 , sub AS v3 
	 FROM sorg__keywords$$4$$
	
	) tab3
 ON(tab5.v3=tab3.v3)
 JOIN    (SELECT obj AS v4 , sub AS v3 
	 FROM sorg__trailer$$3$$
	
	) tab2
 ON(tab3.v3=tab2.v3)
 JOIN    (SELECT obj AS v0 , sub AS v3 
	 FROM wsdbm__hasGenre$$5$$
	
	) tab4
 ON(tab2.v3=tab4.v3)
 JOIN    (SELECT sub AS v0 
	 FROM og__tag$$1$$ 
	 WHERE obj = 'wsdbm:Topic179'
	) tab0
 ON(tab4.v0=tab0.v0)
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM rdf__type$$2$$
	) tab1
 ON(tab0.v0=tab1.v0)


++++++Tables Statistic
sorg__trailer$$3$$	1	SS	sorg__trailer/sorg__keywords
	VP	<sorg__trailer>	17
	SS	<sorg__trailer><sorg__keywords>	2	0.12
	SS	<sorg__trailer><wsdbm__hasGenre>	17	1.0
	SS	<sorg__trailer><rdf__type>	17	1.0
------
sorg__keywords$$4$$	1	SS	sorg__keywords/sorg__trailer
	VP	<sorg__keywords>	769
	SS	<sorg__keywords><sorg__trailer>	1	0.0
	SS	<sorg__keywords><wsdbm__hasGenre>	769	1.0
	SS	<sorg__keywords><rdf__type>	769	1.0
------
rdf__type$$2$$	2	SO	rdf__type/wsdbm__hasGenre
	VP	<rdf__type>	14856
	SS	<rdf__type><og__tag>	1635	0.11
	SO	<rdf__type><wsdbm__hasGenre>	140	0.01
------
wsdbm__hasGenre$$5$$	3	SS	wsdbm__hasGenre/sorg__trailer
	VP	<wsdbm__hasGenre>	5961
	OS	<wsdbm__hasGenre><og__tag>	5308	0.89
	OS	<wsdbm__hasGenre><rdf__type>	5961	1.0
	SS	<wsdbm__hasGenre><sorg__trailer>	26	0.0
	SS	<wsdbm__hasGenre><sorg__keywords>	1806	0.3
	SS	<wsdbm__hasGenre><rdf__type>	5961	1.0
------
rdf__type$$6$$	1	SS	rdf__type/sorg__trailer
	VP	<rdf__type>	14856
	SS	<rdf__type><sorg__trailer>	12	0.0
	SS	<rdf__type><sorg__keywords>	769	0.05
	SS	<rdf__type><wsdbm__hasGenre>	2500	0.17
------
og__tag$$1$$	2	SO	og__tag/wsdbm__hasGenre
	VP	<og__tag>	15121
	SS	<og__tag><rdf__type>	15121	1.0
	SO	<og__tag><wsdbm__hasGenre>	342	0.02
------
