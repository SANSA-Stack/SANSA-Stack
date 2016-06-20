SELECT tab0.person AS person , tab5.state AS state , tab7.date2 AS date2 , tab2.gender AS gender , tab9.property AS property , tab1.personType AS personType , tab8.capital AS capital , tab4.date1 AS date1 , tab6.country AS country , tab3.city AS city 
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
 JOIN    (SELECT sub AS person , obj AS gender 
	 FROM _L_hasGender_B_$$3$$
	) tab2
 ON(tab3.person=tab2.person)
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


++++++Tables Statistic
_L_isLocatedIn_B_$$6$$	0	VP	_L_isLocatedIn_B_/
	VP	<isLocatedIn>	1262926
------
_L_wasCreatedOnDate_B_$$8$$	0	VP	_L_wasCreatedOnDate_B_/
	VP	<wasCreatedOnDate>	722657
------
_L_wasBornIn_B_$$4$$	0	VP	_L_wasBornIn_B_/
	VP	<wasBornIn>	218757
------
rdf__type$$1$$	0	VP	rdf__type/
	VP	<rdf__type>	61165359
------
_L_hasGender_B_$$3$$	0	VP	_L_hasGender_B_/
	VP	<hasGender>	923364
------
skos__prefLabel$$2$$	0	VP	skos__prefLabel/
	VP	<skos__prefLabel>	2954875
------
_L_isLocatedIn_B_$$7$$	0	VP	_L_isLocatedIn_B_/
	VP	<isLocatedIn>	1262926
------
_L_wasDestroyedOnDate_B_$$5$$	0	VP	_L_wasDestroyedOnDate_B_/
	VP	<wasDestroyedOnDate>	43975
------
_L_owns_B_$$10$$	0	VP	_L_owns_B_/
	VP	<owns>	26551
------
_L_hasCapital_B_$$9$$	0	VP	_L_hasCapital_B_/
	VP	<hasCapital>	1937
------
