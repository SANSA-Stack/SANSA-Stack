SELECT tab0.v0 AS v0 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM rdf__type$$1$$ 
	 WHERE obj = 'wsdbm:ProductCategory0'
	) tab0
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM sorg__caption$$2$$
	
	) tab1
 ON(tab0.v0=tab1.v0)
 JOIN    (SELECT sub AS v0 , obj AS v4 
	 FROM sorg__publisher$$4$$
	
	) tab3
 ON(tab1.v0=tab3.v0)
 JOIN    (SELECT sub AS v0 , obj AS v3 
	 FROM wsdbm__hasGenre$$3$$
	
	) tab2
 ON(tab3.v0=tab2.v0)


++++++Tables Statistic
sorg__caption$$2$$	3	SS	sorg__caption/sorg__publisher
	VP	<sorg__caption>	2501
	SS	<sorg__caption><rdf__type>	2501	1.0
	SS	<sorg__caption><wsdbm__hasGenre>	2501	1.0
	SS	<sorg__caption><sorg__publisher>	128	0.05
------
wsdbm__hasGenre$$3$$	3	SS	wsdbm__hasGenre/sorg__publisher
	VP	<wsdbm__hasGenre>	58787
	SS	<wsdbm__hasGenre><rdf__type>	58787	1.0
	SS	<wsdbm__hasGenre><sorg__caption>	5918	0.1
	SS	<wsdbm__hasGenre><sorg__publisher>	3187	0.05
------
sorg__publisher$$4$$	2	SS	sorg__publisher/sorg__caption
	VP	<sorg__publisher>	1350
	SS	<sorg__publisher><rdf__type>	1350	1.0
	SS	<sorg__publisher><sorg__caption>	128	0.09
	SS	<sorg__publisher><wsdbm__hasGenre>	1350	1.0
------
rdf__type$$1$$	3	SS	rdf__type/sorg__publisher
	VP	<rdf__type>	136215
	SS	<rdf__type><sorg__caption>	2501	0.02
	SS	<rdf__type><wsdbm__hasGenre>	25000	0.18
	SS	<rdf__type><sorg__publisher>	1350	0.01
------
