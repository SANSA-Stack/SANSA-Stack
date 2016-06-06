SELECT tab0.GivenName AS GivenName , tab7.a AS a , tab4.p AS p , tab6.switzerland AS switzerland , tab9.germany AS germany , tab2.scientist AS scientist , tab1.FamilyName AS FamilyName , tab8.city2 AS city2 , tab5.city AS city 
 FROM    (SELECT sub AS switzerland 
	 FROM skos__prefLabel$$7$$
	 
	 WHERE obj = '"Switzerland"@eng'
	) tab6
 JOIN    (SELECT obj AS switzerland , sub AS city 
	 FROM _L_isLocatedIn_B_$$6$$
	
	) tab5
 ON(tab6.switzerland=tab5.switzerland)
 JOIN    (SELECT sub AS p , obj AS city 
	 FROM _L_wasBornIn_B_$$5$$
	) tab4
 ON(tab5.city=tab4.city)
 JOIN    (SELECT obj AS a , sub AS p 
	 FROM _L_hasAcademicAdvisor_B_$$8$$
	
	) tab7
 ON(tab4.p=tab7.p)
 JOIN    (SELECT sub AS a , obj AS city2 
	 FROM _L_wasBornIn_B_$$9$$
	) tab8
 ON(tab7.a=tab8.a)
 JOIN    (SELECT obj AS GivenName , sub AS p 
	 FROM _L_hasGivenName_B_$$1$$
	
	) tab0
 ON(tab7.p=tab0.p)
 JOIN    (SELECT sub AS p , obj AS FamilyName 
	 FROM _L_hasFamilyName_B_$$2$$
	
	) tab1
 ON(tab0.p=tab1.p)
 JOIN    (SELECT obj AS germany , sub AS city2 
	 FROM _L_isLocatedIn_B_$$10$$
	
	) tab9
 ON(tab8.city2=tab9.city2)
 JOIN    (SELECT sub AS germany 
	 FROM skos__prefLabel$$11$$
	 
	 WHERE obj = '"Germany"@eng'
	) tab10
 ON(tab9.germany=tab10.germany)
 JOIN    (SELECT sub AS p , obj AS scientist 
	 FROM rdf__type$$3$$
	) tab2
 ON(tab1.p=tab2.p)
 JOIN    (SELECT sub AS scientist 
	 FROM rdfs__label$$4$$ 
	 WHERE obj = '"scientist"@eng'
	) tab3
 ON(tab2.scientist=tab3.scientist)


++++++Tables Statistic
rdfs__label$$4$$	0	VP	rdfs__label/
	VP	<rdfs__label>	20236705
------
_L_isLocatedIn_B_$$6$$	0	VP	_L_isLocatedIn_B_/
	VP	<isLocatedIn>	1262926
------
_L_hasAcademicAdvisor_B_$$8$$	0	VP	_L_hasAcademicAdvisor_B_/
	VP	<hasAcademicAdvisor>	3340
------
_L_isLocatedIn_B_$$10$$	0	VP	_L_isLocatedIn_B_/
	VP	<isLocatedIn>	1262926
------
skos__prefLabel$$7$$	0	VP	skos__prefLabel/
	VP	<skos__prefLabel>	2954875
------
_L_wasBornIn_B_$$5$$	0	VP	_L_wasBornIn_B_/
	VP	<wasBornIn>	218757
------
rdf__type$$3$$	0	VP	rdf__type/
	VP	<rdf__type>	61165359
------
_L_hasGivenName_B_$$1$$	0	VP	_L_hasGivenName_B_/
	VP	<hasGivenName>	827681
------
_L_wasBornIn_B_$$9$$	0	VP	_L_wasBornIn_B_/
	VP	<wasBornIn>	218757
------
_L_hasFamilyName_B_$$2$$	0	VP	_L_hasFamilyName_B_/
	VP	<hasFamilyName>	838669
------
skos__prefLabel$$11$$	0	VP	skos__prefLabel/
	VP	<skos__prefLabel>	2954875
------
