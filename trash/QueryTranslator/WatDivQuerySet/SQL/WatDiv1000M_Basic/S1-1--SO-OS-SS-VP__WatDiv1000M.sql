SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab3.v4 AS v4 , tab5.v6 AS v6 , tab2.v3 AS v3 , tab8.v9 AS v9 , tab7.v8 AS v8 
 FROM    (SELECT obj AS v0 
	 FROM gr__offers$$2$$ 
	 WHERE sub = 'wsdbm:Retailer22436'
	) tab1
 JOIN    (SELECT sub AS v0 , obj AS v5 
	 FROM gr__validFrom$$5$$
	
	) tab4
 ON(tab1.v0=tab4.v0)
 JOIN    (SELECT sub AS v0 , obj AS v9 
	 FROM sorg__priceValidUntil$$9$$
	
	) tab8
 ON(tab4.v0=tab8.v0)
 JOIN    (SELECT sub AS v0 , obj AS v6 
	 FROM gr__validThrough$$6$$
	
	) tab5
 ON(tab8.v0=tab5.v0)
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM gr__includes$$1$$
	) tab0
 ON(tab5.v0=tab0.v0)
 JOIN    (SELECT sub AS v0 , obj AS v3 
	 FROM gr__price$$3$$
	) tab2
 ON(tab0.v0=tab2.v0)
 JOIN    (SELECT sub AS v0 , obj AS v4 
	 FROM gr__serialNumber$$4$$
	
	) tab3
 ON(tab2.v0=tab3.v0)
 JOIN    (SELECT sub AS v0 , obj AS v7 
	 FROM sorg__eligibleQuantity$$7$$
	
	) tab6
 ON(tab3.v0=tab6.v0)
 JOIN    (SELECT sub AS v0 , obj AS v8 
	 FROM sorg__eligibleRegion$$8$$
	
	) tab7
 ON(tab6.v0=tab7.v0)


++++++Tables Statistic
gr__validThrough$$6$$	8	SS	gr__validThrough/sorg__priceValidUntil
	VP	<gr__validThrough>	3602004
	SS	<gr__validThrough><gr__includes>	3602004	1.0
	SO	<gr__validThrough><gr__offers>	1667315	0.46
	SS	<gr__validThrough><gr__price>	3602004	1.0
	SS	<gr__validThrough><gr__serialNumber>	3602004	1.0
	SS	<gr__validThrough><gr__validFrom>	1450006	0.4
	SS	<gr__validThrough><sorg__eligibleQuantity>	3602004	1.0
	SS	<gr__validThrough><sorg__eligibleRegion>	1802799	0.5
	SS	<gr__validThrough><sorg__priceValidUntil>	721777	0.2
------
gr__includes$$1$$	8	SS	gr__includes/sorg__priceValidUntil
	VP	<gr__includes>	9000000
	SO	<gr__includes><gr__offers>	4166185	0.46
	SS	<gr__includes><gr__price>	9000000	1.0
	SS	<gr__includes><gr__serialNumber>	9000000	1.0
	SS	<gr__includes><gr__validFrom>	3595844	0.4
	SS	<gr__includes><gr__validThrough>	3602004	0.4
	SS	<gr__includes><sorg__eligibleQuantity>	9000000	1.0
	SS	<gr__includes><sorg__eligibleRegion>	4505491	0.5
	SS	<gr__includes><sorg__priceValidUntil>	1801266	0.2
------
sorg__priceValidUntil$$9$$	5	SS	sorg__priceValidUntil/gr__validFrom
	VP	<sorg__priceValidUntil>	1801266
	SS	<sorg__priceValidUntil><gr__includes>	1801266	1.0
	SO	<sorg__priceValidUntil><gr__offers>	834597	0.46
	SS	<sorg__priceValidUntil><gr__price>	1801266	1.0
	SS	<sorg__priceValidUntil><gr__serialNumber>	1801266	1.0
	SS	<sorg__priceValidUntil><gr__validFrom>	707113	0.39
	SS	<sorg__priceValidUntil><gr__validThrough>	721777	0.4
	SS	<sorg__priceValidUntil><sorg__eligibleQuantity>	1801266	1.0
	SS	<sorg__priceValidUntil><sorg__eligibleRegion>	902506	0.5
------
gr__offers$$2$$	8	OS	gr__offers/sorg__priceValidUntil
	VP	<gr__offers>	14156906
	OS	<gr__offers><gr__includes>	14156906	1.0
	OS	<gr__offers><gr__price>	14156906	1.0
	OS	<gr__offers><gr__serialNumber>	14156906	1.0
	OS	<gr__offers><gr__validFrom>	5620001	0.4
	OS	<gr__offers><gr__validThrough>	6134667	0.43
	OS	<gr__offers><sorg__eligibleQuantity>	14156906	1.0
	OS	<gr__offers><sorg__eligibleRegion>	6858176	0.48
	OS	<gr__offers><sorg__priceValidUntil>	2926692	0.21
------
sorg__eligibleQuantity$$7$$	8	SS	sorg__eligibleQuantity/sorg__priceValidUntil
	VP	<sorg__eligibleQuantity>	9000000
	SS	<sorg__eligibleQuantity><gr__includes>	9000000	1.0
	SO	<sorg__eligibleQuantity><gr__offers>	4166185	0.46
	SS	<sorg__eligibleQuantity><gr__price>	9000000	1.0
	SS	<sorg__eligibleQuantity><gr__serialNumber>	9000000	1.0
	SS	<sorg__eligibleQuantity><gr__validFrom>	3595844	0.4
	SS	<sorg__eligibleQuantity><gr__validThrough>	3602004	0.4
	SS	<sorg__eligibleQuantity><sorg__eligibleRegion>	4505491	0.5
	SS	<sorg__eligibleQuantity><sorg__priceValidUntil>	1801266	0.2
------
sorg__eligibleRegion$$8$$	8	SS	sorg__eligibleRegion/sorg__priceValidUntil
	VP	<sorg__eligibleRegion>	22527455
	SS	<sorg__eligibleRegion><gr__includes>	22527455	1.0
	SO	<sorg__eligibleRegion><gr__offers>	10426365	0.46
	SS	<sorg__eligibleRegion><gr__price>	22527455	1.0
	SS	<sorg__eligibleRegion><gr__serialNumber>	22527455	1.0
	SS	<sorg__eligibleRegion><gr__validFrom>	9004375	0.4
	SS	<sorg__eligibleRegion><gr__validThrough>	9013995	0.4
	SS	<sorg__eligibleRegion><sorg__eligibleQuantity>	22527455	1.0
	SS	<sorg__eligibleRegion><sorg__priceValidUntil>	4512530	0.2
------
gr__validFrom$$5$$	8	SS	gr__validFrom/sorg__priceValidUntil
	VP	<gr__validFrom>	3595844
	SS	<gr__validFrom><gr__includes>	3595844	1.0
	SO	<gr__validFrom><gr__offers>	1664182	0.46
	SS	<gr__validFrom><gr__price>	3595844	1.0
	SS	<gr__validFrom><gr__serialNumber>	3595844	1.0
	SS	<gr__validFrom><gr__validThrough>	1450006	0.4
	SS	<gr__validFrom><sorg__eligibleQuantity>	3595844	1.0
	SS	<gr__validFrom><sorg__eligibleRegion>	1800875	0.5
	SS	<gr__validFrom><sorg__priceValidUntil>	707113	0.2
------
gr__price$$3$$	8	SS	gr__price/sorg__priceValidUntil
	VP	<gr__price>	24000000
	SS	<gr__price><gr__includes>	9000000	0.38
	SO	<gr__price><gr__offers>	4166185	0.17
	SS	<gr__price><gr__serialNumber>	9000000	0.38
	SS	<gr__price><gr__validFrom>	3595844	0.15
	SS	<gr__price><gr__validThrough>	3602004	0.15
	SS	<gr__price><sorg__eligibleQuantity>	9000000	0.38
	SS	<gr__price><sorg__eligibleRegion>	4505491	0.19
	SS	<gr__price><sorg__priceValidUntil>	1801266	0.08
------
gr__serialNumber$$4$$	8	SS	gr__serialNumber/sorg__priceValidUntil
	VP	<gr__serialNumber>	9000000
	SS	<gr__serialNumber><gr__includes>	9000000	1.0
	SO	<gr__serialNumber><gr__offers>	4166185	0.46
	SS	<gr__serialNumber><gr__price>	9000000	1.0
	SS	<gr__serialNumber><gr__validFrom>	3595844	0.4
	SS	<gr__serialNumber><gr__validThrough>	3602004	0.4
	SS	<gr__serialNumber><sorg__eligibleQuantity>	9000000	1.0
	SS	<gr__serialNumber><sorg__eligibleRegion>	4505491	0.5
	SS	<gr__serialNumber><sorg__priceValidUntil>	1801266	0.2
------
