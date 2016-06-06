SELECT tab2.p2 AS p2 , tab2.p1 AS p1 , tab0.name1 AS name1 , tab1.name2 AS name2 , tab3.city AS city 
 FROM    (SELECT obj AS p2 , sub AS p1 
	 FROM _L_isMarriedTo_B_$$3$$
	
	) tab2
 JOIN    (SELECT sub AS p1 , obj AS city 
	 FROM _L_wasBornIn_B_$$4$$
	) tab3
 ON(tab2.p1=tab3.p1)
 JOIN    (SELECT sub AS p2 , obj AS city 
	 FROM _L_wasBornIn_B_$$5$$
	) tab4
 ON(tab2.p2=tab4.p2 AND tab3.city=tab4.city)
 JOIN    (SELECT sub AS p1 , obj AS name1 
	 FROM skos__prefLabel$$1$$
	
	) tab0
 ON(tab3.p1=tab0.p1)
 JOIN    (SELECT sub AS p2 , obj AS name2 
	 FROM skos__prefLabel$$2$$
	
	) tab1
 ON(tab4.p2=tab1.p2)


++++++Tables Statistic
_L_wasBornIn_B_$$4$$	0	VP	_L_wasBornIn_B_/
	VP	<wasBornIn>	218757
------
_L_wasBornIn_B_$$5$$	0	VP	_L_wasBornIn_B_/
	VP	<wasBornIn>	218757
------
skos__prefLabel$$1$$	0	VP	skos__prefLabel/
	VP	<skos__prefLabel>	2954875
------
_L_isMarriedTo_B_$$3$$	0	VP	_L_isMarriedTo_B_/
	VP	<isMarriedTo>	26325
------
skos__prefLabel$$2$$	0	VP	skos__prefLabel/
	VP	<skos__prefLabel>	2954875
------
