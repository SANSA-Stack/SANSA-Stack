SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab3.v4 AS v4 , tab5.v6 AS v6 , tab2.v3 AS v3 , tab8.v9 AS v9 , tab7.v8 AS v8 
 FROM    (SELECT obj AS v0 
	 FROM gr__offers$$2$$ 
	 WHERE sub = 'wsdbm:Retailer46'
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
	VP	<gr__validThrough>	3625
	SS	<gr__validThrough><gr__includes>	3625	1.0
	SO	<gr__validThrough><gr__offers>	1932	0.53
	SS	<gr__validThrough><gr__price>	3625	1.0
	SS	<gr__validThrough><gr__serialNumber>	3625	1.0
	SS	<gr__validThrough><gr__validFrom>	1483	0.41
	SS	<gr__validThrough><sorg__eligibleQuantity>	3625	1.0
	SS	<gr__validThrough><sorg__eligibleRegion>	1809	0.5
	SS	<gr__validThrough><sorg__priceValidUntil>	711	0.2
------
gr__includes$$1$$	8	SS	gr__includes/sorg__priceValidUntil
	VP	<gr__includes>	9000
	SO	<gr__includes><gr__offers>	4722	0.52
	SS	<gr__includes><gr__price>	9000	1.0
	SS	<gr__includes><gr__serialNumber>	9000	1.0
	SS	<gr__includes><gr__validFrom>	3645	0.41
	SS	<gr__includes><gr__validThrough>	3625	0.4
	SS	<gr__includes><sorg__eligibleQuantity>	9000	1.0
	SS	<gr__includes><sorg__eligibleRegion>	4531	0.5
	SS	<gr__includes><sorg__priceValidUntil>	1751	0.19
------
sorg__priceValidUntil$$9$$	5	SS	sorg__priceValidUntil/gr__validFrom
	VP	<sorg__priceValidUntil>	1751
	SS	<sorg__priceValidUntil><gr__includes>	1751	1.0
	SO	<sorg__priceValidUntil><gr__offers>	953	0.54
	SS	<sorg__priceValidUntil><gr__price>	1751	1.0
	SS	<sorg__priceValidUntil><gr__serialNumber>	1751	1.0
	SS	<sorg__priceValidUntil><gr__validFrom>	695	0.4
	SS	<sorg__priceValidUntil><gr__validThrough>	711	0.41
	SS	<sorg__priceValidUntil><sorg__eligibleQuantity>	1751	1.0
	SS	<sorg__priceValidUntil><sorg__eligibleRegion>	868	0.5
------
gr__offers$$2$$	8	OS	gr__offers/sorg__priceValidUntil
	VP	<gr__offers>	14179
	OS	<gr__offers><gr__includes>	14179	1.0
	OS	<gr__offers><gr__price>	14179	1.0
	OS	<gr__offers><gr__serialNumber>	14179	1.0
	OS	<gr__offers><gr__validFrom>	5880	0.41
	OS	<gr__offers><gr__validThrough>	5625	0.4
	OS	<gr__offers><sorg__eligibleQuantity>	14179	1.0
	OS	<gr__offers><sorg__eligibleRegion>	6524	0.46
	OS	<gr__offers><sorg__priceValidUntil>	2470	0.17
------
sorg__eligibleQuantity$$7$$	8	SS	sorg__eligibleQuantity/sorg__priceValidUntil
	VP	<sorg__eligibleQuantity>	9000
	SS	<sorg__eligibleQuantity><gr__includes>	9000	1.0
	SO	<sorg__eligibleQuantity><gr__offers>	4722	0.52
	SS	<sorg__eligibleQuantity><gr__price>	9000	1.0
	SS	<sorg__eligibleQuantity><gr__serialNumber>	9000	1.0
	SS	<sorg__eligibleQuantity><gr__validFrom>	3645	0.41
	SS	<sorg__eligibleQuantity><gr__validThrough>	3625	0.4
	SS	<sorg__eligibleQuantity><sorg__eligibleRegion>	4531	0.5
	SS	<sorg__eligibleQuantity><sorg__priceValidUntil>	1751	0.19
------
sorg__eligibleRegion$$8$$	8	SS	sorg__eligibleRegion/sorg__priceValidUntil
	VP	<sorg__eligibleRegion>	22655
	SS	<sorg__eligibleRegion><gr__includes>	22655	1.0
	SO	<sorg__eligibleRegion><gr__offers>	11765	0.52
	SS	<sorg__eligibleRegion><gr__price>	22655	1.0
	SS	<sorg__eligibleRegion><gr__serialNumber>	22655	1.0
	SS	<sorg__eligibleRegion><gr__validFrom>	9130	0.4
	SS	<sorg__eligibleRegion><gr__validThrough>	9045	0.4
	SS	<sorg__eligibleRegion><sorg__eligibleQuantity>	22655	1.0
	SS	<sorg__eligibleRegion><sorg__priceValidUntil>	4340	0.19
------
gr__validFrom$$5$$	8	SS	gr__validFrom/sorg__priceValidUntil
	VP	<gr__validFrom>	3645
	SS	<gr__validFrom><gr__includes>	3645	1.0
	SO	<gr__validFrom><gr__offers>	1912	0.52
	SS	<gr__validFrom><gr__price>	3645	1.0
	SS	<gr__validFrom><gr__serialNumber>	3645	1.0
	SS	<gr__validFrom><gr__validThrough>	1483	0.41
	SS	<gr__validFrom><sorg__eligibleQuantity>	3645	1.0
	SS	<gr__validFrom><sorg__eligibleRegion>	1826	0.5
	SS	<gr__validFrom><sorg__priceValidUntil>	695	0.19
------
gr__price$$3$$	8	SS	gr__price/sorg__priceValidUntil
	VP	<gr__price>	24000
	SS	<gr__price><gr__includes>	9000	0.38
	SO	<gr__price><gr__offers>	4722	0.2
	SS	<gr__price><gr__serialNumber>	9000	0.38
	SS	<gr__price><gr__validFrom>	3645	0.15
	SS	<gr__price><gr__validThrough>	3625	0.15
	SS	<gr__price><sorg__eligibleQuantity>	9000	0.38
	SS	<gr__price><sorg__eligibleRegion>	4531	0.19
	SS	<gr__price><sorg__priceValidUntil>	1751	0.07
------
gr__serialNumber$$4$$	8	SS	gr__serialNumber/sorg__priceValidUntil
	VP	<gr__serialNumber>	9000
	SS	<gr__serialNumber><gr__includes>	9000	1.0
	SO	<gr__serialNumber><gr__offers>	4722	0.52
	SS	<gr__serialNumber><gr__price>	9000	1.0
	SS	<gr__serialNumber><gr__validFrom>	3645	0.41
	SS	<gr__serialNumber><gr__validThrough>	3625	0.4
	SS	<gr__serialNumber><sorg__eligibleQuantity>	9000	1.0
	SS	<gr__serialNumber><sorg__eligibleRegion>	4531	0.5
	SS	<gr__serialNumber><sorg__priceValidUntil>	1751	0.19
------
