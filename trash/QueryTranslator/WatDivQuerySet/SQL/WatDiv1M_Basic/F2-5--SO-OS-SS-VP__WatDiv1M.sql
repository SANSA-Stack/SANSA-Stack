SELECT tab0.v1 AS v1 , tab7.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM wsdbm__hasGenre$$8$$
	 
	 WHERE obj = 'wsdbm:SubGenre7'
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
	VP	<sorg__url>	500
	SO	<sorg__url><foaf__homepage>	493	0.99
	SS	<sorg__url><wsdbm__hits>	500	1.0
------
sorg__description$$5$$	4	SS	sorg__description/sorg__caption
	VP	<sorg__description>	1534
	SS	<sorg__description><foaf__homepage>	362	0.24
	SS	<sorg__description><og__title>	1534	1.0
	SS	<sorg__description><rdf__type>	1534	1.0
	SS	<sorg__description><sorg__caption>	136	0.09
	SS	<sorg__description><wsdbm__hasGenre>	1534	1.0
------
wsdbm__hasGenre$$8$$	4	SS	wsdbm__hasGenre/sorg__caption
	VP	<wsdbm__hasGenre>	5961
	SS	<wsdbm__hasGenre><foaf__homepage>	1373	0.23
	SS	<wsdbm__hasGenre><og__title>	5961	1.0
	SS	<wsdbm__hasGenre><rdf__type>	5961	1.0
	SS	<wsdbm__hasGenre><sorg__caption>	541	0.09
	SS	<wsdbm__hasGenre><sorg__description>	3692	0.62
------
rdf__type$$3$$	3	SS	rdf__type/sorg__caption
	VP	<rdf__type>	14856
	SS	<rdf__type><foaf__homepage>	1175	0.08
	SS	<rdf__type><og__title>	2500	0.17
	SS	<rdf__type><sorg__caption>	225	0.02
	SS	<rdf__type><sorg__description>	1534	0.1
	SS	<rdf__type><wsdbm__hasGenre>	2500	0.17
------
wsdbm__hits$$7$$	1	SO	wsdbm__hits/foaf__homepage
	VP	<wsdbm__hits>	500
	SO	<wsdbm__hits><foaf__homepage>	493	0.99
	SS	<wsdbm__hits><sorg__url>	500	1.0
------
sorg__caption$$4$$	1	SS	sorg__caption/foaf__homepage
	VP	<sorg__caption>	225
	SS	<sorg__caption><foaf__homepage>	54	0.24
	SS	<sorg__caption><og__title>	225	1.0
	SS	<sorg__caption><rdf__type>	225	1.0
	SS	<sorg__caption><sorg__description>	136	0.6
	SS	<sorg__caption><wsdbm__hasGenre>	225	1.0
------
og__title$$2$$	3	SS	og__title/sorg__caption
	VP	<og__title>	2500
	SS	<og__title><foaf__homepage>	587	0.23
	SS	<og__title><rdf__type>	2500	1.0
	SS	<og__title><sorg__caption>	225	0.09
	SS	<og__title><sorg__description>	1534	0.61
	SS	<og__title><wsdbm__hasGenre>	2500	1.0
------
foaf__homepage$$1$$	3	SS	foaf__homepage/sorg__caption
	VP	<foaf__homepage>	1068
	SS	<foaf__homepage><og__title>	587	0.55
	SS	<foaf__homepage><rdf__type>	1068	1.0
	SS	<foaf__homepage><sorg__caption>	54	0.05
	SS	<foaf__homepage><sorg__description>	362	0.34
	OS	<foaf__homepage><sorg__url>	1068	1.0
	OS	<foaf__homepage><wsdbm__hits>	1068	1.0
	SS	<foaf__homepage><wsdbm__hasGenre>	587	0.55
------
