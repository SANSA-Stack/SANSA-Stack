SELECT tab1.born AS born , tab0.died AS died , tab3.name1 AS name1 , tab0.date AS date , tab2.name2 AS name2 
 FROM    (SELECT sub AS died , obj AS date 
	 FROM _L_diedOnDate_B_$$1$$
	) tab0
 JOIN    (SELECT sub AS born , obj AS date 
	 FROM _L_wasBornOnDate_B_$$2$$
	
	) tab1
 ON(tab0.date=tab1.date)
 JOIN    (SELECT sub AS died , obj AS name2 
	 FROM skos__prefLabel$$3$$
	
	) tab2
 ON(tab0.died=tab2.died)
 JOIN    (SELECT sub AS born , obj AS name1 
	 FROM skos__prefLabel$$4$$
	
	) tab3
 ON(tab1.born=tab3.born)


++++++Tables Statistic
skos__prefLabel$$4$$	0	VP	skos__prefLabel/
	VP	<skos__prefLabel>	2954875
------
skos__prefLabel$$3$$	0	VP	skos__prefLabel/
	VP	<skos__prefLabel>	2954875
------
_L_diedOnDate_B_$$1$$	0	VP	_L_diedOnDate_B_/
	VP	<diedOnDate>	361933
------
_L_wasBornOnDate_B_$$2$$	0	VP	_L_wasBornOnDate_B_/
	VP	<wasBornOnDate>	805594
------
