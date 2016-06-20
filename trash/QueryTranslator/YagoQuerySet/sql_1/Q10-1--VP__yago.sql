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
 JOIN    (SELECT obj AS child , sub AS person 
	 FROM _L_hasChild_B_$$6$$
	) tab5
 ON(tab3.person=tab5.person)
 JOIN    (SELECT sub AS person , obj AS uni 
	 FROM _L_graduatedFrom_B_$$5$$
	
	) tab4
 ON(tab5.person=tab4.person)
 JOIN    (SELECT obj AS work , sub AS person 
	 FROM _L_wasBornOnDate_B_$$7$$
	
	) tab6
 ON(tab4.person=tab6.person)


++++++Tables Statistic
_L_wasBornOnDate_B_$$7$$	0	VP	_L_wasBornOnDate_B_/
	VP	<wasBornOnDate>	805594
------
rdf__type$$1$$	0	VP	rdf__type/
	VP	<rdf__type>	61165359
------
rdf__type$$3$$	0	VP	rdf__type/
	VP	<rdf__type>	61165359
------
skos__prefLabel$$2$$	0	VP	skos__prefLabel/
	VP	<skos__prefLabel>	2954875
------
_L_graduatedFrom_B_$$5$$	0	VP	_L_graduatedFrom_B_/
	VP	<graduatedFrom>	30389
------
_L_hasChild_B_$$6$$	0	VP	_L_hasChild_B_/
	VP	<hasChild>	17695
------
rdf__type$$4$$	0	VP	rdf__type/
	VP	<rdf__type>	61165359
------
