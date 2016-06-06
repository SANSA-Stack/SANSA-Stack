SELECT tab0.v0 AS v0 , tab3.v5 AS v5 , tab2.v4 AS v4 , tab4.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM og__tag$$1$$ 
	 WHERE obj = 'wsdbm:Topic244'
	) tab0
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM rdf__type$$2$$
	) tab1
 ON(tab0.v0=tab1.v0)
 JOIN    (SELECT obj AS v0 , sub AS v3 
	 FROM wsdbm__hasGenre$$5$$
	
	) tab4
 ON(tab1.v0=tab4.v0)
 JOIN    (SELECT sub AS v3 
	 FROM rdf__type$$6$$ 
	 WHERE obj = 'wsdbm:ProductCategory2'
	) tab5
 ON(tab4.v3=tab5.v3)
 JOIN    (SELECT obj AS v5 , sub AS v3 
	 FROM sorg__keywords$$4$$
	
	) tab3
 ON(tab5.v3=tab3.v3)
 JOIN    (SELECT obj AS v4 , sub AS v3 
	 FROM sorg__trailer$$3$$
	
	) tab2
 ON(tab3.v3=tab2.v3)


++++++Tables Statistic
sorg__trailer$$3$$	1	SS	sorg__trailer/sorg__keywords
	VP	<sorg__trailer>	2406
	SS	<sorg__trailer><sorg__keywords>	748	0.31
	SS	<sorg__trailer><wsdbm__hasGenre>	2406	1.0
	SS	<sorg__trailer><rdf__type>	2406	1.0
------
sorg__keywords$$4$$	1	SS	sorg__keywords/sorg__trailer
	VP	<sorg__keywords>	75175
	SS	<sorg__keywords><sorg__trailer>	402	0.01
	SS	<sorg__keywords><wsdbm__hasGenre>	75175	1.0
	SS	<sorg__keywords><rdf__type>	75175	1.0
------
rdf__type$$2$$	2	SO	rdf__type/wsdbm__hasGenre
	VP	<rdf__type>	1480374
	SS	<rdf__type><og__tag>	149841	0.1
	SO	<rdf__type><wsdbm__hasGenre>	145	0.0
------
wsdbm__hasGenre$$5$$	3	SS	wsdbm__hasGenre/sorg__trailer
	VP	<wsdbm__hasGenre>	593924
	OS	<wsdbm__hasGenre><og__tag>	543810	0.92
	OS	<wsdbm__hasGenre><rdf__type>	593924	1.0
	SS	<wsdbm__hasGenre><sorg__trailer>	3172	0.01
	SS	<wsdbm__hasGenre><sorg__keywords>	178684	0.3
	SS	<wsdbm__hasGenre><rdf__type>	593924	1.0
------
rdf__type$$6$$	1	SS	rdf__type/sorg__trailer
	VP	<rdf__type>	1480374
	SS	<rdf__type><sorg__trailer>	1330	0.0
	SS	<rdf__type><sorg__keywords>	75175	0.05
	SS	<rdf__type><wsdbm__hasGenre>	250000	0.17
------
og__tag$$1$$	2	SO	og__tag/wsdbm__hasGenre
	VP	<og__tag>	1500803
	SS	<og__tag><rdf__type>	1500803	1.0
	SO	<og__tag><wsdbm__hasGenre>	407	0.0
------
