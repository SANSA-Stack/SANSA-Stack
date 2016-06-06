SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 
 FROM    (SELECT obj AS v0 
	 FROM gr__offers$$2$$ 
	 WHERE sub = 'wsdbm:Retailer70688'
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
gr__includes$$1$$	3	SS	gr__includes/gr__validThrough
	VP	<gr__includes>	9000000
	SO	<gr__includes><gr__offers>	4166185	0.46
	SS	<gr__includes><gr__price>	9000000	1.0
	SS	<gr__includes><gr__validThrough>	3602004	0.4
	OS	<gr__includes><og__title>	9000000	1.0
	OS	<gr__includes><rdf__type>	9000000	1.0
------
gr__offers$$2$$	3	OS	gr__offers/gr__validThrough
	VP	<gr__offers>	14156906
	OS	<gr__offers><gr__includes>	14156906	1.0
	OS	<gr__offers><gr__price>	14156906	1.0
	OS	<gr__offers><gr__validThrough>	6134667	0.43
------
og__title$$5$$	1	SO	og__title/gr__includes
	VP	<og__title>	2500000
	SO	<og__title><gr__includes>	2432193	0.97
	SS	<og__title><rdf__type>	2500000	1.0
------
rdf__type$$6$$	1	SO	rdf__type/gr__includes
	VP	<rdf__type>	14800449
	SO	<rdf__type><gr__includes>	2432193	0.16
	SS	<rdf__type><og__title>	2500000	0.17
------
gr__validThrough$$4$$	2	SO	gr__validThrough/gr__offers
	VP	<gr__validThrough>	3602004
	SS	<gr__validThrough><gr__includes>	3602004	1.0
	SO	<gr__validThrough><gr__offers>	1667315	0.46
	SS	<gr__validThrough><gr__price>	3602004	1.0
------
gr__price$$3$$	3	SS	gr__price/gr__validThrough
	VP	<gr__price>	24000000
	SS	<gr__price><gr__includes>	9000000	0.38
	SO	<gr__price><gr__offers>	4166185	0.17
	SS	<gr__price><gr__validThrough>	3602004	0.15
------
