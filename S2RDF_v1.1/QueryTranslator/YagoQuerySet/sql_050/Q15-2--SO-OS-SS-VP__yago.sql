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
_L_hasChild_B_$$5$$	1	SO	_L_hasChild_B_/_L_influences_B_
	VP	<hasChild>	17695
	SO	<hasChild><influences>	334	0.02
	OS	<hasChild><isAffiliatedTo>	977	0.06
------
_L_influences_B_$$4$$	2	OS	_L_influences_B_/_L_hasChild_B_
	VP	<influences>	26306
	SO	<influences><isMarriedTo>	3636	0.14
	OS	<influences><hasChild>	674	0.03
------
_L_hasWebsite_B_$$8$$	1	SO	_L_hasWebsite_B_/_L_isLocatedIn_B_
	VP	<hasWebsite>	226393
	SO	<hasWebsite><isLocatedIn>	19043	0.08
------
rdf__type$$1$$	2	SS	rdf__type/_L_isMarriedTo_B_
	VP	<rdf__type>	61165359
	OS	<rdf__type><skos__prefLabel>	42863266	0.7
	SS	<rdf__type><isMarriedTo>	780229	0.01
------
_L_isMarriedTo_B_$$3$$	2	OS	_L_isMarriedTo_B_/_L_influences_B_
	VP	<isMarriedTo>	26325
	SS	<isMarriedTo><rdf__type>	26324	1.0
	OS	<isMarriedTo><influences>	709	0.03
------
skos__prefLabel$$2$$	1	SO	skos__prefLabel/rdf__type
	VP	<skos__prefLabel>	2954875
	SO	<skos__prefLabel><rdf__type>	8678	0.0
------
_L_isLocatedIn_B_$$7$$	1	SO	_L_isLocatedIn_B_/_L_isAffiliatedTo_B_
	VP	<isLocatedIn>	1262926
	SO	<isLocatedIn><isAffiliatedTo>	4243	0.0
	OS	<isLocatedIn><hasWebsite>	507794	0.4
------
_L_isAffiliatedTo_B_$$6$$	1	SO	_L_isAffiliatedTo_B_/_L_hasChild_B_
	VP	<isAffiliatedTo>	497263
	SO	<isAffiliatedTo><hasChild>	961	0.0
	OS	<isAffiliatedTo><isLocatedIn>	54750	0.11
------
