SELECT tab8.p2 AS p2 , tab2.p1 AS p1 , tab5.award AS award , tab4.scientist AS scientist , tab0.name1 AS name1 , tab1.name2 AS name2 , tab7.city AS city 
 FROM    (SELECT sub AS scientist 
	 FROM rdfs__label$$5$$ 
	 WHERE obj = '"scientist"@eng'
	) tab4
 JOIN    (SELECT sub AS p1 , obj AS scientist 
	 FROM rdf__type$$3$$
	) tab2
 ON(tab4.scientist=tab2.scientist)
 JOIN    (SELECT sub AS p1 , obj AS city 
	 FROM _L_wasBornIn_B_$$8$$
	) tab7
 ON(tab2.p1=tab7.p1)
 JOIN    (SELECT sub AS p2 , obj AS city 
	 FROM _L_wasBornIn_B_$$9$$
	) tab8
 ON(tab7.city=tab8.city)
 JOIN    (SELECT sub AS p1 , obj AS name1 
	 FROM _L_hasFamilyName_B_$$1$$
	
	) tab0
 ON(tab7.p1=tab0.p1)
 JOIN    (SELECT sub AS p2 , obj AS name2 
	 FROM _L_hasFamilyName_B_$$2$$
	
	) tab1
 ON(tab8.p2=tab1.p2)
 JOIN    (SELECT sub AS p1 , obj AS award 
	 FROM _L_hasWonPrize_B_$$6$$
	
	) tab5
 ON(tab0.p1=tab5.p1)
 JOIN    (SELECT sub AS p2 , obj AS award 
	 FROM _L_hasWonPrize_B_$$7$$
	
	) tab6
 ON(tab1.p2=tab6.p2 AND tab5.award=tab6.award)
 JOIN    (SELECT sub AS p2 , obj AS scientist 
	 FROM rdf__type$$4$$
	) tab3
 ON(tab6.p2=tab3.p2 AND tab2.scientist=tab3.scientist)
 
 WHERE (tab2.p1 != tab8.p2)

++++++Tables Statistic
_L_hasFamilyName_B_$$1$$	2	SS	_L_hasFamilyName_B_/_L_hasWonPrize_B_
	VP	<hasFamilyName>	838669
	SS	<hasFamilyName><rdf__type>	838669	1.0
	SS	<hasFamilyName><hasWonPrize>	38247	0.05
	SS	<hasFamilyName><wasBornIn>	165728	0.2
------
_L_wasBornIn_B_$$8$$	3	SS	_L_wasBornIn_B_/_L_hasWonPrize_B_
	VP	<wasBornIn>	218757
	SS	<wasBornIn><hasFamilyName>	191643	0.88
	SS	<wasBornIn><rdf__type>	218756	1.0
	SS	<wasBornIn><hasWonPrize>	17232	0.08
------
_L_hasWonPrize_B_$$7$$	0	VP	_L_hasWonPrize_B_/
	VP	<hasWonPrize>	73763
	SS	<hasWonPrize><hasFamilyName>	65862	0.89
	SS	<hasWonPrize><rdf__type>	73763	1.0
	SS	<hasWonPrize><wasBornIn>	28613	0.39
------
rdfs__label$$5$$	1	SO	rdfs__label/rdf__type
	VP	<rdfs__label>	20236705
	SO	<rdfs__label><rdf__type>	291465	0.01
	SO	<rdfs__label><rdf__type>	291465	0.01
------
rdf__type$$3$$	3	SS	rdf__type/_L_hasWonPrize_B_
	VP	<rdf__type>	61165359
	SS	<rdf__type><hasFamilyName>	24344611	0.4
	OS	<rdf__type><rdfs__label>	44121234	0.72
	SS	<rdf__type><hasWonPrize>	1487073	0.02
	SS	<rdf__type><wasBornIn>	6515295	0.11
------
_L_hasWonPrize_B_$$6$$	0	VP	_L_hasWonPrize_B_/
	VP	<hasWonPrize>	73763
	SS	<hasWonPrize><hasFamilyName>	65862	0.89
	SS	<hasWonPrize><rdf__type>	73763	1.0
	SS	<hasWonPrize><wasBornIn>	28613	0.39
------
_L_wasBornIn_B_$$9$$	3	SS	_L_wasBornIn_B_/_L_hasWonPrize_B_
	VP	<wasBornIn>	218757
	SS	<wasBornIn><hasFamilyName>	191643	0.88
	SS	<wasBornIn><rdf__type>	218756	1.0
	SS	<wasBornIn><hasWonPrize>	17232	0.08
------
_L_hasFamilyName_B_$$2$$	2	SS	_L_hasFamilyName_B_/_L_hasWonPrize_B_
	VP	<hasFamilyName>	838669
	SS	<hasFamilyName><rdf__type>	838669	1.0
	SS	<hasFamilyName><hasWonPrize>	38247	0.05
	SS	<hasFamilyName><wasBornIn>	165728	0.2
------
rdf__type$$4$$	3	SS	rdf__type/_L_hasWonPrize_B_
	VP	<rdf__type>	61165359
	SS	<rdf__type><hasFamilyName>	24344611	0.4
	OS	<rdf__type><rdfs__label>	44121234	0.72
	SS	<rdf__type><hasWonPrize>	1487073	0.02
	SS	<rdf__type><wasBornIn>	6515295	0.11
------
