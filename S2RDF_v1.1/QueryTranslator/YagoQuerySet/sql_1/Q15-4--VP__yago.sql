SELECT tab3.person3 AS person3 , tab4.person4 AS person4 , tab0.person1 AS person1 , tab2.person2 AS person2 , tab6.place AS place , tab5.inst AS inst , tab1.personType AS personType , tab7.url AS url 
 FROM    (SELECT sub AS personType 
	 FROM skos__prefLabel$$2$$
	 
	 WHERE obj = '"person"@eng'
	) tab1
 JOIN    (SELECT sub AS person1 , obj AS personType 
	 FROM rdf__type$$1$$
	) tab0
 ON(tab1.personType=tab0.personType)
 JOIN    (SELECT sub AS person1 , obj AS person2 
	 FROM _L_isMarriedTo_B_$$3$$
	
	) tab2
 ON(tab0.person1=tab2.person1)
 JOIN    (SELECT obj AS person3 , sub AS person2 
	 FROM _L_influences_B_$$4$$
	) tab3
 ON(tab2.person2=tab3.person2)
 JOIN    (SELECT sub AS person3 , obj AS person4 
	 FROM _L_hasChild_B_$$5$$
	) tab4
 ON(tab3.person3=tab4.person3)
 JOIN    (SELECT sub AS person4 , obj AS inst 
	 FROM _L_isAffiliatedTo_B_$$6$$
	
	) tab5
 ON(tab4.person4=tab5.person4)
 JOIN    (SELECT obj AS place , sub AS inst 
	 FROM _L_isLocatedIn_B_$$7$$
	
	) tab6
 ON(tab5.inst=tab6.inst)
 JOIN    (SELECT sub AS place , obj AS url 
	 FROM _L_hasWebsite_B_$$8$$
	) tab7
 ON(tab6.place=tab7.place)


++++++Tables Statistic
_L_hasChild_B_$$5$$	0	VP	_L_hasChild_B_/
	VP	<hasChild>	17695
------
_L_influences_B_$$4$$	0	VP	_L_influences_B_/
	VP	<influences>	26306
------
_L_hasWebsite_B_$$8$$	0	VP	_L_hasWebsite_B_/
	VP	<hasWebsite>	226393
------
rdf__type$$1$$	0	VP	rdf__type/
	VP	<rdf__type>	61165359
------
_L_isMarriedTo_B_$$3$$	0	VP	_L_isMarriedTo_B_/
	VP	<isMarriedTo>	26325
------
skos__prefLabel$$2$$	0	VP	skos__prefLabel/
	VP	<skos__prefLabel>	2954875
------
_L_isLocatedIn_B_$$7$$	0	VP	_L_isLocatedIn_B_/
	VP	<isLocatedIn>	1262926
------
_L_isAffiliatedTo_B_$$6$$	0	VP	_L_isAffiliatedTo_B_/
	VP	<isAffiliatedTo>	497263
------
