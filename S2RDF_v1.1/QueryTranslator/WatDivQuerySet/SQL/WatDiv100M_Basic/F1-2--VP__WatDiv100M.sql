SELECT tab4.v0 AS v0 , tab3.v5 AS v5 , tab2.v4 AS v4 , tab5.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v3 
	 FROM rdf__type$$6$$ 
	 WHERE obj = 'wsdbm:ProductCategory2'
	) tab5
 JOIN    (SELECT obj AS v4 , sub AS v3 
	 FROM sorg__trailer$$3$$
	
	) tab2
 ON(tab5.v3=tab2.v3)
 JOIN    (SELECT obj AS v5 , sub AS v3 
	 FROM sorg__keywords$$4$$
	
	) tab3
 ON(tab2.v3=tab3.v3)
 JOIN    (SELECT obj AS v0 , sub AS v3 
	 FROM wsdbm__hasGenre$$5$$
	
	) tab4
 ON(tab3.v3=tab4.v3)
 JOIN    (SELECT sub AS v0 
	 FROM og__tag$$1$$ 
	 WHERE obj = 'wsdbm:Topic8'
	) tab0
 ON(tab4.v0=tab0.v0)
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM rdf__type$$2$$
	) tab1
 ON(tab0.v0=tab1.v0)


++++++Tables Statistic
sorg__trailer$$3$$	0	VP	sorg__trailer/
	VP	<sorg__trailer>	2406
------
sorg__keywords$$4$$	0	VP	sorg__keywords/
	VP	<sorg__keywords>	75175
------
rdf__type$$2$$	0	VP	rdf__type/
	VP	<rdf__type>	1480374
------
wsdbm__hasGenre$$5$$	0	VP	wsdbm__hasGenre/
	VP	<wsdbm__hasGenre>	593924
------
rdf__type$$6$$	0	VP	rdf__type/
	VP	<rdf__type>	1480374
------
og__tag$$1$$	0	VP	og__tag/
	VP	<og__tag>	1500803
------
