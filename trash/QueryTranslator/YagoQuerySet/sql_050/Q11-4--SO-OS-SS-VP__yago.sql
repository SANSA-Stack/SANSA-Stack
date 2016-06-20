SELECT tab0.person AS person , tab5.state AS state , tab2.gender AS gender , tab7.date2 AS date2 , tab9.property AS property , tab1.personType AS personType , tab8.capital AS capital , tab4.date1 AS date1 , tab6.country AS country , tab3.city AS city 
 FROM    (SELECT sub AS personType 
	 FROM skos__prefLabel$$2$$
	 
	 WHERE obj = '"person"@eng'
	) tab1
 JOIN    (SELECT sub AS person , obj AS personType 
	 FROM rdf__type$$1$$
	) tab0
 ON(tab1.personType=tab0.personType)
 JOIN    (SELECT sub AS person , obj AS city 
	 FROM _L_wasBornIn_B_$$4$$
	) tab3
 ON(tab0.person=tab3.person)
 JOIN    (SELECT obj AS date1 , sub AS city 
	 FROM _L_wasDestroyedOnDate_B_$$5$$
	
	) tab4
 ON(tab3.city=tab4.city)
 JOIN    (SELECT obj AS state , sub AS city 
	 FROM _L_isLocatedIn_B_$$6$$
	
	) tab5
 ON(tab4.city=tab5.city)
 JOIN    (SELECT sub AS state , obj AS country 
	 FROM _L_isLocatedIn_B_$$7$$
	
	) tab6
 ON(tab5.state=tab6.state)
 JOIN    (SELECT obj AS capital , sub AS country 
	 FROM _L_hasCapital_B_$$9$$
	) tab8
 ON(tab6.country=tab8.country)
 JOIN    (SELECT obj AS property , sub AS capital 
	 FROM _L_owns_B_$$10$$
	) tab9
 ON(tab8.capital=tab9.capital)
 JOIN    (SELECT obj AS date2 , sub AS country 
	 FROM _L_wasCreatedOnDate_B_$$8$$
	
	) tab7
 ON(tab8.country=tab7.country)
 JOIN    (SELECT sub AS person , obj AS gender 
	 FROM _L_hasGender_B_$$3$$
	) tab2
 ON(tab3.person=tab2.person)


++++++Tables Statistic
_L_isLocatedIn_B_$$6$$	2	SS	_L_isLocatedIn_B_/_L_wasDestroyedOnDate_B_
	VP	<isLocatedIn>	1262926
	SO	<isLocatedIn><wasBornIn>	29918	0.02
	SS	<isLocatedIn><wasDestroyedOnDate>	14581	0.01
	OS	<isLocatedIn><isLocatedIn>	1012822	0.8
------
_L_wasCreatedOnDate_B_$$8$$	2	SS	_L_wasCreatedOnDate_B_/_L_hasCapital_B_
	VP	<wasCreatedOnDate>	722657
	SO	<wasCreatedOnDate><isLocatedIn>	19736	0.03
	SS	<wasCreatedOnDate><hasCapital>	1481	0.0
------
_L_wasBornIn_B_$$4$$	3	OS	_L_wasBornIn_B_/_L_wasDestroyedOnDate_B_
	VP	<wasBornIn>	218757
	SS	<wasBornIn><rdf__type>	218756	1.0
	SS	<wasBornIn><hasGender>	204372	0.93
	OS	<wasBornIn><wasDestroyedOnDate>	1084	0.0
	OS	<wasBornIn><isLocatedIn>	215484	0.99
------
rdf__type$$1$$	3	SS	rdf__type/_L_wasBornIn_B_
	VP	<rdf__type>	61165359
	OS	<rdf__type><skos__prefLabel>	42863266	0.7
	SS	<rdf__type><hasGender>	26830035	0.44
	SS	<rdf__type><wasBornIn>	6515295	0.11
------
_L_hasGender_B_$$3$$	2	SS	_L_hasGender_B_/_L_wasBornIn_B_
	VP	<hasGender>	923364
	SS	<hasGender><rdf__type>	923364	1.0
	SS	<hasGender><wasBornIn>	176466	0.19
------
skos__prefLabel$$2$$	1	SO	skos__prefLabel/rdf__type
	VP	<skos__prefLabel>	2954875
	SO	<skos__prefLabel><rdf__type>	8678	0.0
------
_L_isLocatedIn_B_$$7$$	1	SO	_L_isLocatedIn_B_/_L_isLocatedIn_B_
	VP	<isLocatedIn>	1262926
	SO	<isLocatedIn><isLocatedIn>	104901	0.08
	OS	<isLocatedIn><wasCreatedOnDate>	693101	0.55
	OS	<isLocatedIn><hasCapital>	412079	0.33
------
_L_wasDestroyedOnDate_B_$$5$$	1	SO	_L_wasDestroyedOnDate_B_/_L_wasBornIn_B_
	VP	<wasDestroyedOnDate>	43975
	SO	<wasDestroyedOnDate><wasBornIn>	55	0.0
	SS	<wasDestroyedOnDate><isLocatedIn>	12199	0.28
------
_L_owns_B_$$10$$	1	SO	_L_owns_B_/_L_hasCapital_B_
	VP	<owns>	26551
	SO	<owns><hasCapital>	332	0.01
------
_L_hasCapital_B_$$9$$	3	OS	_L_hasCapital_B_/_L_owns_B_
	VP	<hasCapital>	1937
	SO	<hasCapital><isLocatedIn>	940	0.49
	SS	<hasCapital><wasCreatedOnDate>	1013	0.52
	OS	<hasCapital><owns>	291	0.15
------
