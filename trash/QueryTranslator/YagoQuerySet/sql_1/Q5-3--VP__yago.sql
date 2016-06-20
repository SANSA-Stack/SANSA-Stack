SELECT tab6.p2 AS p2 , tab2.p1 AS p1 , tab5.award AS award , tab4.scientist AS scientist , tab0.name1 AS name1 , tab1.name2 AS name2 , tab7.city AS city 
 FROM    (SELECT sub AS scientist 
	 FROM rdfs__label$$5$$ 
	 WHERE obj = '"scientist"@eng'
	) tab4
 JOIN    (SELECT sub AS p1 , obj AS scientist 
	 FROM rdf__type$$3$$
	) tab2
 ON(tab4.scientist=tab2.scientist)
 JOIN    (SELECT sub AS p1 , obj AS award 
	 FROM _L_hasWonPrize_B_$$6$$
	
	) tab5
 ON(tab2.p1=tab5.p1)
 JOIN    (SELECT sub AS p2 , obj AS award 
	 FROM _L_hasWonPrize_B_$$7$$
	
	) tab6
 ON(tab5.award=tab6.award)
 JOIN    (SELECT sub AS p1 , obj AS city 
	 FROM _L_wasBornIn_B_$$8$$
	) tab7
 ON(tab5.p1=tab7.p1)
 JOIN    (SELECT sub AS p2 , obj AS city 
	 FROM _L_wasBornIn_B_$$9$$
	) tab8
 ON(tab6.p2=tab8.p2 AND tab7.city=tab8.city)
 JOIN    (SELECT sub AS p1 , obj AS name1 
	 FROM _L_hasFamilyName_B_$$1$$
	
	) tab0
 ON(tab7.p1=tab0.p1)
 JOIN    (SELECT sub AS p2 , obj AS name2 
	 FROM _L_hasFamilyName_B_$$2$$
	
	) tab1
 ON(tab8.p2=tab1.p2)
 JOIN    (SELECT sub AS p2 , obj AS scientist 
	 FROM rdf__type$$4$$
	) tab3
 ON(tab1.p2=tab3.p2 AND tab2.scientist=tab3.scientist)
 
 WHERE (tab2.p1 != tab6.p2)

++++++Tables Statistic
_L_hasFamilyName_B_$$1$$	0	VP	_L_hasFamilyName_B_/
	VP	<hasFamilyName>	838669
------
_L_wasBornIn_B_$$8$$	0	VP	_L_wasBornIn_B_/
	VP	<wasBornIn>	218757
------
_L_hasWonPrize_B_$$7$$	0	VP	_L_hasWonPrize_B_/
	VP	<hasWonPrize>	73763
------
rdfs__label$$5$$	0	VP	rdfs__label/
	VP	<rdfs__label>	20236705
------
rdf__type$$3$$	0	VP	rdf__type/
	VP	<rdf__type>	61165359
------
_L_hasWonPrize_B_$$6$$	0	VP	_L_hasWonPrize_B_/
	VP	<hasWonPrize>	73763
------
_L_wasBornIn_B_$$9$$	0	VP	_L_wasBornIn_B_/
	VP	<wasBornIn>	218757
------
_L_hasFamilyName_B_$$2$$	0	VP	_L_hasFamilyName_B_/
	VP	<hasFamilyName>	838669
------
rdf__type$$4$$	0	VP	rdf__type/
	VP	<rdf__type>	61165359
------
