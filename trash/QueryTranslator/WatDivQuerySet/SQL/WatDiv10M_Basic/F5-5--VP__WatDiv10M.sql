SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 
 FROM    (SELECT obj AS v0 
	 FROM gr__offers$$2$$ 
	 WHERE sub = 'wsdbm:Retailer22'
	) tab1
 JOIN    (SELECT sub AS v0 , obj AS v4 
	 FROM gr__validThrough$$4$$
	
	) tab3
 ON(tab1.v0=tab3.v0)
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM gr__includes$$1$$
	) tab0
 ON(tab3.v0=tab0.v0)
 JOIN    (SELECT sub AS v1 , obj AS v5 
	 FROM og__title$$5$$
	) tab4
 ON(tab0.v1=tab4.v1)
 JOIN    (SELECT sub AS v1 , obj AS v6 
	 FROM rdf__type$$6$$
	) tab5
 ON(tab4.v1=tab5.v1)
 JOIN    (SELECT sub AS v0 , obj AS v3 
	 FROM gr__price$$3$$
	) tab2
 ON(tab0.v0=tab2.v0)


++++++Tables Statistic
gr__includes$$1$$	0	VP	gr__includes/
	VP	<gr__includes>	90000
------
gr__offers$$2$$	0	VP	gr__offers/
	VP	<gr__offers>	119316
------
og__title$$5$$	0	VP	og__title/
	VP	<og__title>	25000
------
rdf__type$$6$$	0	VP	rdf__type/
	VP	<rdf__type>	136215
------
gr__validThrough$$4$$	0	VP	gr__validThrough/
	VP	<gr__validThrough>	36346
------
gr__price$$3$$	0	VP	gr__price/
	VP	<gr__price>	240000
------
