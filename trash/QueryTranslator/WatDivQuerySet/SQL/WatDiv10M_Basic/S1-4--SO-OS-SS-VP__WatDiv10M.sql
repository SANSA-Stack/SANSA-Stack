SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab3.v4 AS v4 , tab5.v6 AS v6 , tab2.v3 AS v3 , tab8.v9 AS v9 , tab7.v8 AS v8 
 FROM    (SELECT obj AS v0 
	 FROM gr__offers$$2$$ 
	 WHERE sub = 'wsdbm:Retailer1126'
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
	VP	<gr__validThrough>	36346
	SS	<gr__validThrough><gr__includes>	36346	1.0
	SO	<gr__validThrough><gr__offers>	17984	0.49
	SS	<gr__validThrough><gr__price>	36346	1.0
	SS	<gr__validThrough><gr__serialNumber>	36346	1.0
	SS	<gr__validThrough><gr__validFrom>	14812	0.41
	SS	<gr__validThrough><sorg__eligibleQuantity>	36346	1.0
	SS	<gr__validThrough><sorg__eligibleRegion>	18074	0.5
	SS	<gr__validThrough><sorg__priceValidUntil>	7212	0.2
------
gr__includes$$1$$	8	SS	gr__includes/sorg__priceValidUntil
	VP	<gr__includes>	90000
	SO	<gr__includes><gr__offers>	44841	0.5
	SS	<gr__includes><gr__price>	90000	1.0
	SS	<gr__includes><gr__serialNumber>	90000	1.0
	SS	<gr__includes><gr__validFrom>	36250	0.4
	SS	<gr__includes><gr__validThrough>	36346	0.4
	SS	<gr__includes><sorg__eligibleQuantity>	90000	1.0
	SS	<gr__includes><sorg__eligibleRegion>	44935	0.5
	SS	<gr__includes><sorg__priceValidUntil>	17899	0.2
------
sorg__priceValidUntil$$9$$	5	SS	sorg__priceValidUntil/gr__validFrom
	VP	<sorg__priceValidUntil>	17899
	SS	<sorg__priceValidUntil><gr__includes>	17899	1.0
	SO	<sorg__priceValidUntil><gr__offers>	8850	0.49
	SS	<sorg__priceValidUntil><gr__price>	17899	1.0
	SS	<sorg__priceValidUntil><gr__serialNumber>	17899	1.0
	SS	<sorg__priceValidUntil><gr__validFrom>	6990	0.39
	SS	<sorg__priceValidUntil><gr__validThrough>	7212	0.4
	SS	<sorg__priceValidUntil><sorg__eligibleQuantity>	17899	1.0
	SS	<sorg__priceValidUntil><sorg__eligibleRegion>	8897	0.5
------
gr__offers$$2$$	8	OS	gr__offers/sorg__priceValidUntil
	VP	<gr__offers>	119316
	OS	<gr__offers><gr__includes>	119316	1.0
	OS	<gr__offers><gr__price>	119316	1.0
	OS	<gr__offers><gr__serialNumber>	119316	1.0
	OS	<gr__offers><gr__validFrom>	48256	0.4
	OS	<gr__offers><gr__validThrough>	45983	0.39
	OS	<gr__offers><sorg__eligibleQuantity>	119316	1.0
	OS	<gr__offers><sorg__eligibleRegion>	58569	0.49
	OS	<gr__offers><sorg__priceValidUntil>	25046	0.21
------
sorg__eligibleQuantity$$7$$	8	SS	sorg__eligibleQuantity/sorg__priceValidUntil
	VP	<sorg__eligibleQuantity>	90000
	SS	<sorg__eligibleQuantity><gr__includes>	90000	1.0
	SO	<sorg__eligibleQuantity><gr__offers>	44841	0.5
	SS	<sorg__eligibleQuantity><gr__price>	90000	1.0
	SS	<sorg__eligibleQuantity><gr__serialNumber>	90000	1.0
	SS	<sorg__eligibleQuantity><gr__validFrom>	36250	0.4
	SS	<sorg__eligibleQuantity><gr__validThrough>	36346	0.4
	SS	<sorg__eligibleQuantity><sorg__eligibleRegion>	44935	0.5
	SS	<sorg__eligibleQuantity><sorg__priceValidUntil>	17899	0.2
------
sorg__eligibleRegion$$8$$	8	SS	sorg__eligibleRegion/sorg__priceValidUntil
	VP	<sorg__eligibleRegion>	183550
	SS	<sorg__eligibleRegion><gr__includes>	183550	1.0
	SO	<sorg__eligibleRegion><gr__offers>	91392	0.5
	SS	<sorg__eligibleRegion><gr__price>	183550	1.0
	SS	<sorg__eligibleRegion><gr__serialNumber>	183550	1.0
	SS	<sorg__eligibleRegion><gr__validFrom>	73313	0.4
	SS	<sorg__eligibleRegion><gr__validThrough>	73698	0.4
	SS	<sorg__eligibleRegion><sorg__eligibleQuantity>	183550	1.0
	SS	<sorg__eligibleRegion><sorg__priceValidUntil>	36360	0.2
------
gr__validFrom$$5$$	8	SS	gr__validFrom/sorg__priceValidUntil
	VP	<gr__validFrom>	36250
	SS	<gr__validFrom><gr__includes>	36250	1.0
	SO	<gr__validFrom><gr__offers>	18197	0.5
	SS	<gr__validFrom><gr__price>	36250	1.0
	SS	<gr__validFrom><gr__serialNumber>	36250	1.0
	SS	<gr__validFrom><gr__validThrough>	14812	0.41
	SS	<gr__validFrom><sorg__eligibleQuantity>	36250	1.0
	SS	<gr__validFrom><sorg__eligibleRegion>	17964	0.5
	SS	<gr__validFrom><sorg__priceValidUntil>	6990	0.19
------
gr__price$$3$$	8	SS	gr__price/sorg__priceValidUntil
	VP	<gr__price>	240000
	SS	<gr__price><gr__includes>	90000	0.38
	SO	<gr__price><gr__offers>	44841	0.19
	SS	<gr__price><gr__serialNumber>	90000	0.38
	SS	<gr__price><gr__validFrom>	36250	0.15
	SS	<gr__price><gr__validThrough>	36346	0.15
	SS	<gr__price><sorg__eligibleQuantity>	90000	0.38
	SS	<gr__price><sorg__eligibleRegion>	44935	0.19
	SS	<gr__price><sorg__priceValidUntil>	17899	0.07
------
gr__serialNumber$$4$$	8	SS	gr__serialNumber/sorg__priceValidUntil
	VP	<gr__serialNumber>	90000
	SS	<gr__serialNumber><gr__includes>	90000	1.0
	SO	<gr__serialNumber><gr__offers>	44841	0.5
	SS	<gr__serialNumber><gr__price>	90000	1.0
	SS	<gr__serialNumber><gr__validFrom>	36250	0.4
	SS	<gr__serialNumber><gr__validThrough>	36346	0.4
	SS	<gr__serialNumber><sorg__eligibleQuantity>	90000	1.0
	SS	<gr__serialNumber><sorg__eligibleRegion>	44935	0.5
	SS	<gr__serialNumber><sorg__priceValidUntil>	17899	0.2
------
