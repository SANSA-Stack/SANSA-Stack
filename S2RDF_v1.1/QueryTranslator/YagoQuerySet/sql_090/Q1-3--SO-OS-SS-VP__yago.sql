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
rdfs__label$$4$$	1	SO	rdfs__label/rdf__type
	VP	<rdfs__label>	20236705
	SO	<rdfs__label><rdf__type>	291465	0.01
------
_L_isLocatedIn_B_$$6$$	1	SO	_L_isLocatedIn_B_/_L_wasBornIn_B_
	VP	<isLocatedIn>	1262926
	SO	<isLocatedIn><wasBornIn>	29918	0.02
	OS	<isLocatedIn><skos__prefLabel>	1262904	1.0
------
_L_hasAcademicAdvisor_B_$$8$$	5	OS	_L_hasAcademicAdvisor_B_/_L_wasBornIn_B_
	VP	<hasAcademicAdvisor>	3340
	SS	<hasAcademicAdvisor><hasGivenName>	3068	0.92
	SS	<hasAcademicAdvisor><hasFamilyName>	3157	0.95
	SS	<hasAcademicAdvisor><rdf__type>	3340	1.0
	SS	<hasAcademicAdvisor><wasBornIn>	1419	0.42
	OS	<hasAcademicAdvisor><wasBornIn>	1388	0.42
------
_L_isLocatedIn_B_$$10$$	1	SO	_L_isLocatedIn_B_/_L_wasBornIn_B_
	VP	<isLocatedIn>	1262926
	SO	<isLocatedIn><wasBornIn>	29918	0.02
	OS	<isLocatedIn><skos__prefLabel>	1262904	1.0
------
skos__prefLabel$$7$$	1	SO	skos__prefLabel/_L_isLocatedIn_B_
	VP	<skos__prefLabel>	2954875
	SO	<skos__prefLabel><isLocatedIn>	59238	0.02
------
_L_wasBornIn_B_$$5$$	5	SS	_L_wasBornIn_B_/_L_hasAcademicAdvisor_B_
	VP	<wasBornIn>	218757
	SS	<wasBornIn><hasGivenName>	189558	0.87
	SS	<wasBornIn><hasFamilyName>	191643	0.88
	SS	<wasBornIn><rdf__type>	218756	1.0
	OS	<wasBornIn><isLocatedIn>	215484	0.99
	SS	<wasBornIn><hasAcademicAdvisor>	1559	0.01
------
rdf__type$$3$$	5	SS	rdf__type/_L_hasAcademicAdvisor_B_
	VP	<rdf__type>	61165359
	SS	<rdf__type><hasGivenName>	24045202	0.39
	SS	<rdf__type><hasFamilyName>	24344611	0.4
	OS	<rdf__type><rdfs__label>	44121234	0.72
	SS	<rdf__type><wasBornIn>	6515295	0.11
	SS	<rdf__type><hasAcademicAdvisor>	111929	0.0
------
_L_hasGivenName_B_$$1$$	4	SS	_L_hasGivenName_B_/_L_hasAcademicAdvisor_B_
	VP	<hasGivenName>	827681
	SS	<hasGivenName><hasFamilyName>	827681	1.0
	SS	<hasGivenName><rdf__type>	827681	1.0
	SS	<hasGivenName><wasBornIn>	163967	0.2
	SS	<hasGivenName><hasAcademicAdvisor>	2914	0.0
------
_L_wasBornIn_B_$$9$$	1	SO	_L_wasBornIn_B_/_L_hasAcademicAdvisor_B_
	VP	<wasBornIn>	218757
	SO	<wasBornIn><hasAcademicAdvisor>	941	0.0
	OS	<wasBornIn><isLocatedIn>	215484	0.99
------
_L_hasFamilyName_B_$$2$$	4	SS	_L_hasFamilyName_B_/_L_hasAcademicAdvisor_B_
	VP	<hasFamilyName>	838669
	SS	<hasFamilyName><hasGivenName>	827681	0.99
	SS	<hasFamilyName><rdf__type>	838669	1.0
	SS	<hasFamilyName><wasBornIn>	165728	0.2
	SS	<hasFamilyName><hasAcademicAdvisor>	2993	0.0
------
skos__prefLabel$$11$$	1	SO	skos__prefLabel/_L_isLocatedIn_B_
	VP	<skos__prefLabel>	2954875
	SO	<skos__prefLabel><isLocatedIn>	59238	0.02
------
