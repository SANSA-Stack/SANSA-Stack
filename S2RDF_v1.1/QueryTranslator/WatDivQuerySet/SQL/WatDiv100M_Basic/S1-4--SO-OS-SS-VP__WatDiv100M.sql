SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab3.v4 AS v4 , tab5.v6 AS v6 , tab2.v3 AS v3 , tab8.v9 AS v9 , tab7.v8 AS v8 
 FROM    (SELECT obj AS v0 
	 FROM gr__offers$$2$$ 
	 WHERE sub = 'wsdbm:Retailer10306'
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
	VP	<gr__validThrough>	359409
	SS	<gr__validThrough><gr__includes>	359409	1.0
	SO	<gr__validThrough><gr__offers>	172491	0.48
	SS	<gr__validThrough><gr__price>	359409	1.0
	SS	<gr__validThrough><gr__serialNumber>	359409	1.0
	SS	<gr__validThrough><gr__validFrom>	145131	0.4
	SS	<gr__validThrough><sorg__eligibleQuantity>	359409	1.0
	SS	<gr__validThrough><sorg__eligibleRegion>	180081	0.5
	SS	<gr__validThrough><sorg__priceValidUntil>	71476	0.2
------
gr__includes$$1$$	8	SS	gr__includes/sorg__priceValidUntil
	VP	<gr__includes>	900000
	SO	<gr__includes><gr__offers>	432735	0.48
	SS	<gr__includes><gr__price>	900000	1.0
	SS	<gr__includes><gr__serialNumber>	900000	1.0
	SS	<gr__includes><gr__validFrom>	359334	0.4
	SS	<gr__includes><gr__validThrough>	359409	0.4
	SS	<gr__includes><sorg__eligibleQuantity>	900000	1.0
	SS	<gr__includes><sorg__eligibleRegion>	450630	0.5
	SS	<gr__includes><sorg__priceValidUntil>	179697	0.2
------
sorg__priceValidUntil$$9$$	5	SS	sorg__priceValidUntil/gr__validFrom
	VP	<sorg__priceValidUntil>	179697
	SS	<sorg__priceValidUntil><gr__includes>	179697	1.0
	SO	<sorg__priceValidUntil><gr__offers>	86244	0.48
	SS	<sorg__priceValidUntil><gr__price>	179697	1.0
	SS	<sorg__priceValidUntil><gr__serialNumber>	179697	1.0
	SS	<sorg__priceValidUntil><gr__validFrom>	70557	0.39
	SS	<sorg__priceValidUntil><gr__validThrough>	71476	0.4
	SS	<sorg__priceValidUntil><sorg__eligibleQuantity>	179697	1.0
	SS	<sorg__priceValidUntil><sorg__eligibleRegion>	90034	0.5
------
gr__offers$$2$$	8	OS	gr__offers/sorg__priceValidUntil
	VP	<gr__offers>	1420053
	OS	<gr__offers><gr__includes>	1420053	1.0
	OS	<gr__offers><gr__price>	1420053	1.0
	OS	<gr__offers><gr__serialNumber>	1420053	1.0
	OS	<gr__offers><gr__validFrom>	509235	0.36
	OS	<gr__offers><gr__validThrough>	509641	0.36
	OS	<gr__offers><sorg__eligibleQuantity>	1420053	1.0
	OS	<gr__offers><sorg__eligibleRegion>	748569	0.53
	OS	<gr__offers><sorg__priceValidUntil>	264374	0.19
------
sorg__eligibleQuantity$$7$$	8	SS	sorg__eligibleQuantity/sorg__priceValidUntil
	VP	<sorg__eligibleQuantity>	900000
	SS	<sorg__eligibleQuantity><gr__includes>	900000	1.0
	SO	<sorg__eligibleQuantity><gr__offers>	432735	0.48
	SS	<sorg__eligibleQuantity><gr__price>	900000	1.0
	SS	<sorg__eligibleQuantity><gr__serialNumber>	900000	1.0
	SS	<sorg__eligibleQuantity><gr__validFrom>	359334	0.4
	SS	<sorg__eligibleQuantity><gr__validThrough>	359409	0.4
	SS	<sorg__eligibleQuantity><sorg__eligibleRegion>	450630	0.5
	SS	<sorg__eligibleQuantity><sorg__priceValidUntil>	179697	0.2
------
sorg__eligibleRegion$$8$$	8	SS	sorg__eligibleRegion/sorg__priceValidUntil
	VP	<sorg__eligibleRegion>	2253150
	SS	<sorg__eligibleRegion><gr__includes>	2253150	1.0
	SO	<sorg__eligibleRegion><gr__offers>	1081460	0.48
	SS	<sorg__eligibleRegion><gr__price>	2253150	1.0
	SS	<sorg__eligibleRegion><gr__serialNumber>	2253150	1.0
	SS	<sorg__eligibleRegion><gr__validFrom>	899770	0.4
	SS	<sorg__eligibleRegion><gr__validThrough>	900405	0.4
	SS	<sorg__eligibleRegion><sorg__eligibleQuantity>	2253150	1.0
	SS	<sorg__eligibleRegion><sorg__priceValidUntil>	450170	0.2
------
gr__validFrom$$5$$	8	SS	gr__validFrom/sorg__priceValidUntil
	VP	<gr__validFrom>	359334
	SS	<gr__validFrom><gr__includes>	359334	1.0
	SO	<gr__validFrom><gr__offers>	172163	0.48
	SS	<gr__validFrom><gr__price>	359334	1.0
	SS	<gr__validFrom><gr__serialNumber>	359334	1.0
	SS	<gr__validFrom><gr__validThrough>	145131	0.4
	SS	<gr__validFrom><sorg__eligibleQuantity>	359334	1.0
	SS	<gr__validFrom><sorg__eligibleRegion>	179954	0.5
	SS	<gr__validFrom><sorg__priceValidUntil>	70557	0.2
------
gr__price$$3$$	8	SS	gr__price/sorg__priceValidUntil
	VP	<gr__price>	2400000
	SS	<gr__price><gr__includes>	900000	0.38
	SO	<gr__price><gr__offers>	432735	0.18
	SS	<gr__price><gr__serialNumber>	900000	0.38
	SS	<gr__price><gr__validFrom>	359334	0.15
	SS	<gr__price><gr__validThrough>	359409	0.15
	SS	<gr__price><sorg__eligibleQuantity>	900000	0.38
	SS	<gr__price><sorg__eligibleRegion>	450630	0.19
	SS	<gr__price><sorg__priceValidUntil>	179697	0.07
------
gr__serialNumber$$4$$	8	SS	gr__serialNumber/sorg__priceValidUntil
	VP	<gr__serialNumber>	900000
	SS	<gr__serialNumber><gr__includes>	900000	1.0
	SO	<gr__serialNumber><gr__offers>	432735	0.48
	SS	<gr__serialNumber><gr__price>	900000	1.0
	SS	<gr__serialNumber><gr__validFrom>	359334	0.4
	SS	<gr__serialNumber><gr__validThrough>	359409	0.4
	SS	<gr__serialNumber><sorg__eligibleQuantity>	900000	1.0
	SS	<gr__serialNumber><sorg__eligibleRegion>	450630	0.5
	SS	<gr__serialNumber><sorg__priceValidUntil>	179697	0.2
------
