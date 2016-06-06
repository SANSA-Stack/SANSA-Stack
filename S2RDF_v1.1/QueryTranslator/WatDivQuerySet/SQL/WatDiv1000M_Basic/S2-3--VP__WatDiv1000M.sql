SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab2.v3 AS v3 
 FROM    (SELECT sub AS v0 
	 FROM sorg__nationality$$2$$
	 
	 WHERE obj = 'wsdbm:Country0'
	) tab1
 JOIN    (SELECT sub AS v0 
	 FROM rdf__type$$4$$ 
	 WHERE obj = 'wsdbm:Role2'
	) tab3
 ON(tab1.v0=tab3.v0)
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM dc__Location$$1$$
	) tab0
 ON(tab3.v0=tab0.v0)
 JOIN    (SELECT sub AS v0 , obj AS v3 
	 FROM wsdbm__gender$$3$$
	
	) tab2
 ON(tab0.v0=tab2.v0)


++++++Tables Statistic
sorg__nationality$$2$$	0	VP	sorg__nationality/
	VP	<sorg__nationality>	2000713
------
wsdbm__gender$$3$$	0	VP	wsdbm__gender/
	VP	<wsdbm__gender>	5999423
------
dc__Location$$1$$	0	VP	dc__Location/
	VP	<dc__Location>	4000049
------
rdf__type$$4$$	0	VP	rdf__type/
	VP	<rdf__type>	14800449
------
