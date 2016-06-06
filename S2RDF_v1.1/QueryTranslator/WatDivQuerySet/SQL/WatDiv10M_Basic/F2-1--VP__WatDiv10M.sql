SELECT tab0.v1 AS v1 , tab7.v0 AS v0 , tab4.v5 AS v5 , tab6.v7 AS v7 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM wsdbm__hasGenre$$8$$
	 
	 WHERE obj = 'wsdbm:SubGenre38'
	) tab7
 JOIN    (SELECT sub AS v0 , obj AS v4 
	 FROM sorg__caption$$4$$
	
	) tab3
 ON(tab7.v0=tab3.v0)
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM foaf__homepage$$1$$
	
	) tab0
 ON(tab3.v0=tab0.v0)
 JOIN    (SELECT sub AS v1 , obj AS v6 
	 FROM sorg__url$$6$$
	) tab5
 ON(tab0.v1=tab5.v1)
 JOIN    (SELECT sub AS v1 , obj AS v7 
	 FROM wsdbm__hits$$7$$
	) tab6
 ON(tab5.v1=tab6.v1)
 JOIN    (SELECT sub AS v0 , obj AS v5 
	 FROM sorg__description$$5$$
	
	) tab4
 ON(tab0.v0=tab4.v0)
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM og__title$$2$$
	) tab1
 ON(tab4.v0=tab1.v0)
 JOIN    (SELECT sub AS v0 , obj AS v3 
	 FROM rdf__type$$3$$
	) tab2
 ON(tab1.v0=tab2.v0)


++++++Tables Statistic
sorg__url$$6$$	0	VP	sorg__url/
	VP	<sorg__url>	5000
------
sorg__description$$5$$	0	VP	sorg__description/
	VP	<sorg__description>	14960
------
wsdbm__hasGenre$$8$$	0	VP	wsdbm__hasGenre/
	VP	<wsdbm__hasGenre>	58787
------
rdf__type$$3$$	0	VP	rdf__type/
	VP	<rdf__type>	136215
------
wsdbm__hits$$7$$	0	VP	wsdbm__hits/
	VP	<wsdbm__hits>	5000
------
sorg__caption$$4$$	0	VP	sorg__caption/
	VP	<sorg__caption>	2501
------
og__title$$2$$	0	VP	og__title/
	VP	<og__title>	25000
------
foaf__homepage$$1$$	0	VP	foaf__homepage/
	VP	<foaf__homepage>	11204
------
