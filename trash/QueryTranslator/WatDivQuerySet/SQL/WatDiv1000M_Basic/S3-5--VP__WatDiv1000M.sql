SELECT tab0.v0 AS v0 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM rdf__type$$1$$ 
	 WHERE obj = 'wsdbm:ProductCategory13'
	) tab0
 JOIN    (SELECT sub AS v0 , obj AS v4 
	 FROM sorg__publisher$$4$$
	
	) tab3
 ON(tab0.v0=tab3.v0)
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM sorg__caption$$2$$
	
	) tab1
 ON(tab3.v0=tab1.v0)
 JOIN    (SELECT sub AS v0 , obj AS v3 
	 FROM wsdbm__hasGenre$$3$$
	
	) tab2
 ON(tab1.v0=tab2.v0)


++++++Tables Statistic
sorg__caption$$2$$	0	VP	sorg__caption/
	VP	<sorg__caption>	250207
------
wsdbm__hasGenre$$3$$	0	VP	wsdbm__hasGenre/
	VP	<wsdbm__hasGenre>	5936571
------
sorg__publisher$$4$$	0	VP	sorg__publisher/
	VP	<sorg__publisher>	133272
------
rdf__type$$1$$	0	VP	rdf__type/
	VP	<rdf__type>	14800449
------
