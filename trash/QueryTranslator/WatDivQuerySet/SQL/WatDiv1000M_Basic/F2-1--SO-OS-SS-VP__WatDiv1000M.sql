SELECT tab0.v1 AS v1 , tab7.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM wsdbm__hasGenre$$8$$
	 
	 WHERE obj = 'wsdbm:SubGenre85'
	) tab7
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM foaf__homepage$$1$$
	
	) tab0
 ON(tab7.v0=tab0.v0)
 JOIN    (SELECT sub AS v0 , obj AS v4 
	 FROM sorg__caption$$4$$
	
	) tab3
 ON(tab0.v0=tab3.v0)
 JOIN    (SELECT sub AS v0 , obj AS v5 
	 FROM sorg__description$$5$$
	
	) tab4
 ON(tab3.v0=tab4.v0)
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM og__title$$2$$
	) tab1
 ON(tab4.v0=tab1.v0)
 JOIN    (SELECT sub AS v0 , obj AS v3 
	 FROM rdf__type$$3$$
	) tab2
 ON(tab1.v0=tab2.v0)
 JOIN    (SELECT sub AS v1 , obj AS v6 
	 FROM sorg__url$$6$$
	) tab5
 ON(tab0.v1=tab5.v1)
 JOIN    (SELECT sub AS v1 , obj AS v7 
	 FROM wsdbm__hits$$7$$
	) tab6
 ON(tab5.v1=tab6.v1)


++++++Tables Statistic
sorg__url$$6$$	1	SO	sorg__url/foaf__homepage
	VP	<sorg__url>	500000
	SO	<sorg__url><foaf__homepage>	497821	1.0
	SS	<sorg__url><wsdbm__hits>	500000	1.0
------
sorg__description$$5$$	4	SS	sorg__description/sorg__caption
	VP	<sorg__description>	1502748
	SS	<sorg__description><foaf__homepage>	376154	0.25
	SS	<sorg__description><og__title>	1502748	1.0
	SS	<sorg__description><rdf__type>	1502748	1.0
	SS	<sorg__description><sorg__caption>	149706	0.1
	SS	<sorg__description><wsdbm__hasGenre>	1502748	1.0
------
wsdbm__hasGenre$$8$$	4	SS	wsdbm__hasGenre/sorg__caption
	VP	<wsdbm__hasGenre>	5936571
	SS	<wsdbm__hasGenre><foaf__homepage>	1486716	0.25
	SS	<wsdbm__hasGenre><og__title>	5936571	1.0
	SS	<wsdbm__hasGenre><rdf__type>	5936571	1.0
	SS	<wsdbm__hasGenre><sorg__caption>	593980	0.1
	SS	<wsdbm__hasGenre><sorg__description>	3567731	0.6
------
rdf__type$$3$$	3	SS	rdf__type/sorg__caption
	VP	<rdf__type>	14800449
	SS	<rdf__type><foaf__homepage>	1231731	0.08
	SS	<rdf__type><og__title>	2500000	0.17
	SS	<rdf__type><sorg__caption>	250207	0.02
	SS	<rdf__type><sorg__description>	1502748	0.1
	SS	<rdf__type><wsdbm__hasGenre>	2500000	0.17
------
wsdbm__hits$$7$$	1	SO	wsdbm__hits/foaf__homepage
	VP	<wsdbm__hits>	500000
	SO	<wsdbm__hits><foaf__homepage>	497821	1.0
	SS	<wsdbm__hits><sorg__url>	500000	1.0
------
sorg__caption$$4$$	1	SS	sorg__caption/foaf__homepage
	VP	<sorg__caption>	250207
	SS	<sorg__caption><foaf__homepage>	62657	0.25
	SS	<sorg__caption><og__title>	250207	1.0
	SS	<sorg__caption><rdf__type>	250207	1.0
	SS	<sorg__caption><sorg__description>	149706	0.6
	SS	<sorg__caption><wsdbm__hasGenre>	250207	1.0
------
og__title$$2$$	3	SS	og__title/sorg__caption
	VP	<og__title>	2500000
	SS	<og__title><foaf__homepage>	626158	0.25
	SS	<og__title><rdf__type>	2500000	1.0
	SS	<og__title><sorg__caption>	250207	0.1
	SS	<og__title><sorg__description>	1502748	0.6
	SS	<og__title><wsdbm__hasGenre>	2500000	1.0
------
foaf__homepage$$1$$	3	SS	foaf__homepage/sorg__caption
	VP	<foaf__homepage>	1118496
	SS	<foaf__homepage><og__title>	626158	0.56
	SS	<foaf__homepage><rdf__type>	1118496	1.0
	SS	<foaf__homepage><sorg__caption>	62657	0.06
	SS	<foaf__homepage><sorg__description>	376154	0.34
	OS	<foaf__homepage><sorg__url>	1118496	1.0
	OS	<foaf__homepage><wsdbm__hits>	1118496	1.0
	SS	<foaf__homepage><wsdbm__hasGenre>	626158	0.56
------
