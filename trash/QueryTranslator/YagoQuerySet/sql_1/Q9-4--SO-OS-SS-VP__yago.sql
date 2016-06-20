SELECT tab1.born AS born , tab2.died AS died , tab3.name1 AS name1 , tab0.date AS date , tab2.name2 AS name2 
 FROM    (SELECT sub AS died , obj AS name2 
	 FROM skos__prefLabel$$3$$
	
	) tab2
 JOIN    (SELECT sub AS died , obj AS date 
	 FROM _L_diedOnDate_B_$$1$$
	) tab0
 ON(tab2.died=tab0.died)
 JOIN    (SELECT sub AS born , obj AS date 
	 FROM _L_wasBornOnDate_B_$$2$$
	
	) tab1
 ON(tab0.date=tab1.date)
 JOIN    (SELECT sub AS born , obj AS name1 
	 FROM skos__prefLabel$$4$$
	
	) tab3
 ON(tab1.born=tab3.born)


++++++Tables Statistic
skos__prefLabel$$4$$	1	SS	skos__prefLabel/_L_wasBornOnDate_B_
	VP	<skos__prefLabel>	2954875
	SS	<skos__prefLabel><wasBornOnDate>	796080	0.27
------
skos__prefLabel$$3$$	1	SS	skos__prefLabel/_L_diedOnDate_B_
	VP	<skos__prefLabel>	2954875
	SS	<skos__prefLabel><diedOnDate>	359527	0.12
------
_L_diedOnDate_B_$$1$$	1	SS	_L_diedOnDate_B_/skos__prefLabel
	VP	<diedOnDate>	361933
	SS	<diedOnDate><skos__prefLabel>	361928	1.0
------
_L_wasBornOnDate_B_$$2$$	1	SS	_L_wasBornOnDate_B_/skos__prefLabel
	VP	<wasBornOnDate>	805594
	SS	<wasBornOnDate><skos__prefLabel>	805584	1.0
------
