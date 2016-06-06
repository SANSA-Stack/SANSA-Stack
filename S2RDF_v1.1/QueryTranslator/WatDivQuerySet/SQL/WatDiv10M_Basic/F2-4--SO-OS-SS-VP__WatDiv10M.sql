SELECT tab0.v1 AS v1 , tab7.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM wsdbm__hasGenre$$8$$
	 
	 WHERE obj = 'wsdbm:SubGenre142'
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
	VP	<sorg__url>	5000
	SO	<sorg__url><foaf__homepage>	4983	1.0
	SS	<sorg__url><wsdbm__hits>	5000	1.0
------
sorg__description$$5$$	4	SS	sorg__description/sorg__caption
	VP	<sorg__description>	14960
	SS	<sorg__description><foaf__homepage>	3760	0.25
	SS	<sorg__description><og__title>	14960	1.0
	SS	<sorg__description><rdf__type>	14960	1.0
	SS	<sorg__description><sorg__caption>	1518	0.1
	SS	<sorg__description><wsdbm__hasGenre>	14960	1.0
------
wsdbm__hasGenre$$8$$	4	SS	wsdbm__hasGenre/sorg__caption
	VP	<wsdbm__hasGenre>	58787
	SS	<wsdbm__hasGenre><foaf__homepage>	14763	0.25
	SS	<wsdbm__hasGenre><og__title>	58787	1.0
	SS	<wsdbm__hasGenre><rdf__type>	58787	1.0
	SS	<wsdbm__hasGenre><sorg__caption>	5918	0.1
	SS	<wsdbm__hasGenre><sorg__description>	35254	0.6
------
rdf__type$$3$$	3	SS	rdf__type/sorg__caption
	VP	<rdf__type>	136215
	SS	<rdf__type><foaf__homepage>	11688	0.09
	SS	<rdf__type><og__title>	25000	0.18
	SS	<rdf__type><sorg__caption>	2501	0.02
	SS	<rdf__type><sorg__description>	14960	0.11
	SS	<rdf__type><wsdbm__hasGenre>	25000	0.18
------
wsdbm__hits$$7$$	1	SO	wsdbm__hits/foaf__homepage
	VP	<wsdbm__hits>	5000
	SO	<wsdbm__hits><foaf__homepage>	4983	1.0
	SS	<wsdbm__hits><sorg__url>	5000	1.0
------
sorg__caption$$4$$	1	SS	sorg__caption/foaf__homepage
	VP	<sorg__caption>	2501
	SS	<sorg__caption><foaf__homepage>	618	0.25
	SS	<sorg__caption><og__title>	2501	1.0
	SS	<sorg__caption><rdf__type>	2501	1.0
	SS	<sorg__caption><sorg__description>	1518	0.61
	SS	<sorg__caption><wsdbm__hasGenre>	2501	1.0
------
og__title$$2$$	3	SS	og__title/sorg__caption
	VP	<og__title>	25000
	SS	<og__title><foaf__homepage>	6274	0.25
	SS	<og__title><rdf__type>	25000	1.0
	SS	<og__title><sorg__caption>	2501	0.1
	SS	<og__title><sorg__description>	14960	0.6
	SS	<og__title><wsdbm__hasGenre>	25000	1.0
------
foaf__homepage$$1$$	3	SS	foaf__homepage/sorg__caption
	VP	<foaf__homepage>	11204
	SS	<foaf__homepage><og__title>	6274	0.56
	SS	<foaf__homepage><rdf__type>	11204	1.0
	SS	<foaf__homepage><sorg__caption>	618	0.06
	SS	<foaf__homepage><sorg__description>	3760	0.34
	OS	<foaf__homepage><sorg__url>	11204	1.0
	OS	<foaf__homepage><wsdbm__hits>	11204	1.0
	SS	<foaf__homepage><wsdbm__hasGenre>	6274	0.56
------
