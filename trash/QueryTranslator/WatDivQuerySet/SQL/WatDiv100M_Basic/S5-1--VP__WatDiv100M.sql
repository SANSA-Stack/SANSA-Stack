SELECT tab3.v0 AS v0 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM sorg__language$$4$$
	 
	 WHERE obj = 'wsdbm:Language0'
	) tab3
 JOIN    (SELECT sub AS v0 
	 FROM rdf__type$$1$$ 
	 WHERE obj = 'wsdbm:ProductCategory7'
	) tab0
 ON(tab3.v0=tab0.v0)
 JOIN    (SELECT sub AS v0 , obj AS v3 
	 FROM sorg__keywords$$3$$
	
	) tab2
 ON(tab0.v0=tab2.v0)
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM sorg__description$$2$$
	
	) tab1
 ON(tab2.v0=tab1.v0)


++++++Tables Statistic
sorg__description$$2$$	0	VP	sorg__description/
	VP	<sorg__description>	150228
------
rdf__type$$1$$	0	VP	rdf__type/
	VP	<rdf__type>	1480374
------
sorg__keywords$$3$$	0	VP	sorg__keywords/
	VP	<sorg__keywords>	75175
------
sorg__language$$4$$	0	VP	sorg__language/
	VP	<sorg__language>	63899
------
