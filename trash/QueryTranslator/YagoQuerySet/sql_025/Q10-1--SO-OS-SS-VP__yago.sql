SELECT tab6.work AS work , tab5.child AS child , tab0.person AS person , tab4.uni AS uni , tab1.personType AS personType 
 FROM    (SELECT sub AS personType 
	 FROM skos__prefLabel$$2$$
	 
	 WHERE obj = '"person"@eng'
	) tab1
 JOIN    (SELECT sub AS person , obj AS personType 
	 FROM rdf__type$$1$$
	) tab0
 ON(tab1.personType=tab0.personType)
 JOIN    (SELECT sub AS person 
	 FROM rdf__type$$3$$ 
	 WHERE obj = '<wikicategory_Cosmologists>'
	) tab2
 ON(tab0.person=tab2.person)
 JOIN    (SELECT sub AS person 
	 FROM rdf__type$$4$$ 
	 WHERE obj = '<wikicategory_German_philosophers>'
	) tab3
 ON(tab2.person=tab3.person)
 JOIN    (SELECT sub AS person , obj AS uni 
	 FROM _L_graduatedFrom_B_$$5$$
	
	) tab4
 ON(tab3.person=tab4.person)
 JOIN    (SELECT obj AS child , sub AS person 
	 FROM _L_hasChild_B_$$6$$
	) tab5
 ON(tab4.person=tab5.person)
 JOIN    (SELECT obj AS work , sub AS person 
	 FROM _L_wasBornOnDate_B_$$7$$
	
	) tab6
 ON(tab5.person=tab6.person)


++++++Tables Statistic
_L_wasBornOnDate_B_$$7$$	5	SS	_L_wasBornOnDate_B_/_L_hasChild_B_
	VP	<wasBornOnDate>	805594
	SS	<wasBornOnDate><rdf__type>	805592	1.0
	SS	<wasBornOnDate><rdf__type>	805592	1.0
	SS	<wasBornOnDate><rdf__type>	805592	1.0
	SS	<wasBornOnDate><graduatedFrom>	27977	0.03
	SS	<wasBornOnDate><hasChild>	9388	0.01
------
rdf__type$$1$$	5	SS	rdf__type/_L_hasChild_B_
	VP	<rdf__type>	61165359
	OS	<rdf__type><skos__prefLabel>	42863266	0.7
	SS	<rdf__type><rdf__type>	61165359	1.0
	SS	<rdf__type><rdf__type>	61165359	1.0
	SS	<rdf__type><graduatedFrom>	1089554	0.02
	SS	<rdf__type><hasChild>	362800	0.01
	SS	<rdf__type><wasBornOnDate>	24022813	0.39
------
rdf__type$$3$$	4	SS	rdf__type/_L_hasChild_B_
	VP	<rdf__type>	61165359
	SS	<rdf__type><rdf__type>	61165359	1.0
	SS	<rdf__type><rdf__type>	61165359	1.0
	SS	<rdf__type><graduatedFrom>	1089554	0.02
	SS	<rdf__type><hasChild>	362800	0.01
	SS	<rdf__type><wasBornOnDate>	24022813	0.39
------
skos__prefLabel$$2$$	1	SO	skos__prefLabel/rdf__type
	VP	<skos__prefLabel>	2954875
	SO	<skos__prefLabel><rdf__type>	8678	0.0
------
_L_graduatedFrom_B_$$5$$	4	SS	_L_graduatedFrom_B_/_L_hasChild_B_
	VP	<graduatedFrom>	30389
	SS	<graduatedFrom><rdf__type>	30389	1.0
	SS	<graduatedFrom><rdf__type>	30389	1.0
	SS	<graduatedFrom><rdf__type>	30389	1.0
	SS	<graduatedFrom><hasChild>	773	0.03
	SS	<graduatedFrom><wasBornOnDate>	27547	0.91
------
_L_hasChild_B_$$6$$	4	SS	_L_hasChild_B_/_L_graduatedFrom_B_
	VP	<hasChild>	17695
	SS	<hasChild><rdf__type>	17695	1.0
	SS	<hasChild><rdf__type>	17695	1.0
	SS	<hasChild><rdf__type>	17695	1.0
	SS	<hasChild><graduatedFrom>	1158	0.07
	SS	<hasChild><wasBornOnDate>	15422	0.87
------
rdf__type$$4$$	4	SS	rdf__type/_L_hasChild_B_
	VP	<rdf__type>	61165359
	SS	<rdf__type><rdf__type>	61165359	1.0
	SS	<rdf__type><rdf__type>	61165359	1.0
	SS	<rdf__type><graduatedFrom>	1089554	0.02
	SS	<rdf__type><hasChild>	362800	0.01
	SS	<rdf__type><wasBornOnDate>	24022813	0.39
------
