SELECT tab3.country1 AS country1 , tab4.country2 AS country2 , tab1.cityType AS cityType , tab5.continent AS continent , tab2.state AS state , tab0.city AS city 
 FROM    (SELECT sub AS cityType 
	 FROM skos__prefLabel$$2$$
	 
	 WHERE obj = '"city"@eng'
	) tab1
 JOIN    (SELECT obj AS cityType , sub AS city 
	 FROM rdf__type$$1$$
	) tab0
 ON(tab1.cityType=tab0.cityType)
 JOIN    (SELECT obj AS state , sub AS city 
	 FROM _L_isLocatedIn_B_$$3$$
	
	) tab2
 ON(tab0.city=tab2.city)
 JOIN    (SELECT obj AS country1 , sub AS state 
	 FROM _L_isLocatedIn_B_$$4$$
	
	) tab3
 ON(tab2.state=tab3.state)
 JOIN    (SELECT sub AS country1 , obj AS country2 
	 FROM _L_dealsWith_B_$$5$$
	) tab4
 ON(tab3.country1=tab4.country1)
 JOIN    (SELECT sub AS country2 , obj AS continent 
	 FROM _L_isLocatedIn_B_$$6$$
	
	) tab5
 ON(tab4.country2=tab5.country2)


++++++Tables Statistic
_L_isLocatedIn_B_$$6$$	0	VP	_L_isLocatedIn_B_/
	VP	<isLocatedIn>	1262926
------
_L_isLocatedIn_B_$$3$$	0	VP	_L_isLocatedIn_B_/
	VP	<isLocatedIn>	1262926
------
_L_isLocatedIn_B_$$4$$	0	VP	_L_isLocatedIn_B_/
	VP	<isLocatedIn>	1262926
------
rdf__type$$1$$	0	VP	rdf__type/
	VP	<rdf__type>	61165359
------
_L_dealsWith_B_$$5$$	0	VP	_L_dealsWith_B_/
	VP	<dealsWith>	947
------
skos__prefLabel$$2$$	0	VP	skos__prefLabel/
	VP	<skos__prefLabel>	2954875
------
