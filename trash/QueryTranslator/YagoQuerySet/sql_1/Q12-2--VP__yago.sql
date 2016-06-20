SELECT tab2.country1 AS country1 , tab6.person AS person , tab4.player AS player , tab7.country2 AS country2 , tab1.cityType AS cityType , tab3.inst AS inst , tab0.capital AS capital , tab8.population AS population , tab5.city AS city 
 FROM    (SELECT sub AS cityType 
	 FROM skos__prefLabel$$2$$
	 
	 WHERE obj = '"city"@eng'
	) tab1
 JOIN    (SELECT obj AS cityType , sub AS capital 
	 FROM rdf__type$$1$$
	) tab0
 ON(tab1.cityType=tab0.cityType)
 JOIN    (SELECT sub AS country1 , obj AS capital 
	 FROM _L_hasCapital_B_$$3$$
	) tab2
 ON(tab0.capital=tab2.capital)
 JOIN    (SELECT obj AS inst , sub AS capital 
	 FROM _L_linksTo_B_$$4$$
	) tab3
 ON(tab2.capital=tab3.capital)
 JOIN    (SELECT sub AS player , obj AS inst 
	 FROM _L_playsFor_B_$$5$$
	) tab4
 ON(tab3.inst=tab4.inst)
 JOIN    (SELECT sub AS player , obj AS city 
	 FROM _L_wasBornIn_B_$$6$$
	) tab5
 ON(tab4.player=tab5.player)
 JOIN    (SELECT sub AS person , obj AS city 
	 FROM _L_diedIn_B_$$7$$
	) tab6
 ON(tab5.city=tab6.city)
 JOIN    (SELECT sub AS person , obj AS country2 
	 FROM _L_isCitizenOf_B_$$8$$
	
	) tab7
 ON(tab6.person=tab7.person)
 JOIN    (SELECT sub AS country2 , obj AS population 
	 FROM _L_hasNumberOfPeople_B_$$9$$
	
	) tab8
 ON(tab7.country2=tab8.country2)


++++++Tables Statistic
_L_hasNumberOfPeople_B_$$9$$	0	VP	_L_hasNumberOfPeople_B_/
	VP	<hasNumberOfPeople>	230745
------
_L_playsFor_B_$$5$$	0	VP	_L_playsFor_B_/
	VP	<playsFor>	412388
------
_L_hasCapital_B_$$3$$	0	VP	_L_hasCapital_B_/
	VP	<hasCapital>	1937
------
rdf__type$$1$$	0	VP	rdf__type/
	VP	<rdf__type>	61165359
------
_L_isCitizenOf_B_$$8$$	0	VP	_L_isCitizenOf_B_/
	VP	<isCitizenOf>	46060
------
skos__prefLabel$$2$$	0	VP	skos__prefLabel/
	VP	<skos__prefLabel>	2954875
------
_L_diedIn_B_$$7$$	0	VP	_L_diedIn_B_/
	VP	<diedIn>	54174
------
_L_linksTo_B_$$4$$	0	VP	_L_linksTo_B_/
	VP	<linksTo>	38048450
------
_L_wasBornIn_B_$$6$$	0	VP	_L_wasBornIn_B_/
	VP	<wasBornIn>	218757
------
