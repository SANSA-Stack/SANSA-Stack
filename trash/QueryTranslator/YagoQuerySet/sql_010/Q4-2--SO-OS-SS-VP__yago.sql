SELECT tab4.p2 AS p2 , tab3.p1 AS p1 , tab0.name1 AS name1 , tab1.name2 AS name2 , tab4.city AS city 
 FROM    (SELECT sub AS p2 , obj AS city 
	 FROM _L_wasBornIn_B_$$5$$
	) tab4
 JOIN    (SELECT sub AS p1 , obj AS city 
	 FROM _L_wasBornIn_B_$$4$$
	) tab3
 ON(tab4.city=tab3.city)
 JOIN    (SELECT sub AS p1 , obj AS name1 
	 FROM skos__prefLabel$$1$$
	
	) tab0
 ON(tab3.p1=tab0.p1)
 JOIN    (SELECT sub AS p2 , obj AS name2 
	 FROM skos__prefLabel$$2$$
	
	) tab1
 ON(tab4.p2=tab1.p2)
 JOIN    (SELECT obj AS p2 , sub AS p1 
	 FROM _L_isMarriedTo_B_$$3$$
	
	) tab2
 ON(tab1.p2=tab2.p2 AND tab0.p1=tab2.p1)


++++++Tables Statistic
_L_wasBornIn_B_$$4$$	2	SS	_L_wasBornIn_B_/_L_isMarriedTo_B_
	VP	<wasBornIn>	218757
	SS	<wasBornIn><skos__prefLabel>	218755	1.0
	SS	<wasBornIn><isMarriedTo>	9637	0.04
------
_L_wasBornIn_B_$$5$$	2	SO	_L_wasBornIn_B_/_L_isMarriedTo_B_
	VP	<wasBornIn>	218757
	SS	<wasBornIn><skos__prefLabel>	218755	1.0
	SO	<wasBornIn><isMarriedTo>	9636	0.04
------
skos__prefLabel$$1$$	1	SS	skos__prefLabel/_L_isMarriedTo_B_
	VP	<skos__prefLabel>	2954875
	SS	<skos__prefLabel><isMarriedTo>	22593	0.01
	SS	<skos__prefLabel><wasBornIn>	189090	0.06
------
_L_isMarriedTo_B_$$3$$	0	VP	_L_isMarriedTo_B_/
	VP	<isMarriedTo>	26325
	SS	<isMarriedTo><skos__prefLabel>	26324	1.0
	OS	<isMarriedTo><skos__prefLabel>	26324	1.0
	SS	<isMarriedTo><wasBornIn>	9430	0.36
	OS	<isMarriedTo><wasBornIn>	9420	0.36
------
skos__prefLabel$$2$$	1	SO	skos__prefLabel/_L_isMarriedTo_B_
	VP	<skos__prefLabel>	2954875
	SO	<skos__prefLabel><isMarriedTo>	22595	0.01
	SS	<skos__prefLabel><wasBornIn>	189090	0.06
------
