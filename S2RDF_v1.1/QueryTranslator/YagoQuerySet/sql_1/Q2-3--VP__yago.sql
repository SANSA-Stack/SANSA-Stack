SELECT tab0.name AS name , tab3.a AS a , tab6.m1 AS m1 , tab4.movie AS movie , tab7.m2 AS m2 , tab1.actor AS actor 
 FROM    (SELECT sub AS m1 
	 FROM _L_isLocatedIn_B_$$7$$
	 
	 WHERE obj = '<Germany>'
	) tab6
 JOIN    (SELECT sub AS a , obj AS m1 
	 FROM _L_actedIn_B_$$4$$
	) tab3
 ON(tab6.m1=tab3.m1)
 JOIN    (SELECT sub AS a , obj AS m2 
	 FROM _L_directed_B_$$8$$
	) tab7
 ON(tab3.a=tab7.a)
 JOIN    (SELECT sub AS m2 
	 FROM _L_isLocatedIn_B_$$10$$
	 
	 WHERE obj = '<United_States>'
	) tab9
 ON(tab7.m2=tab9.m2)
 JOIN    (SELECT obj AS name , sub AS a 
	 FROM skos__prefLabel$$1$$
	
	) tab0
 ON(tab7.a=tab0.a)
 JOIN    (SELECT sub AS a , obj AS actor 
	 FROM rdf__type$$2$$
	) tab1
 ON(tab0.a=tab1.a)
 JOIN    (SELECT sub AS actor 
	 FROM rdfs__label$$3$$ 
	 WHERE obj = '"actor"@eng'
	) tab2
 ON(tab1.actor=tab2.actor)
 JOIN    (SELECT sub AS m1 , obj AS movie 
	 FROM rdf__type$$5$$
	) tab4
 ON(tab3.m1=tab4.m1)
 JOIN    (SELECT sub AS movie 
	 FROM rdfs__label$$6$$ 
	 WHERE obj = '"movie"@eng'
	) tab5
 ON(tab4.movie=tab5.movie)
 JOIN    (SELECT obj AS movie , sub AS m2 
	 FROM rdf__type$$9$$
	) tab8
 ON(tab5.movie=tab8.movie AND tab9.m2=tab8.m2)


++++++Tables Statistic
rdf__type$$5$$	0	VP	rdf__type/
	VP	<rdf__type>	61165359
------
rdfs__label$$6$$	0	VP	rdfs__label/
	VP	<rdfs__label>	20236705
------
rdf__type$$2$$	0	VP	rdf__type/
	VP	<rdf__type>	61165359
------
_L_actedIn_B_$$4$$	0	VP	_L_actedIn_B_/
	VP	<actedIn>	127513
------
_L_isLocatedIn_B_$$10$$	0	VP	_L_isLocatedIn_B_/
	VP	<isLocatedIn>	1262926
------
skos__prefLabel$$1$$	0	VP	skos__prefLabel/
	VP	<skos__prefLabel>	2954875
------
_L_isLocatedIn_B_$$7$$	0	VP	_L_isLocatedIn_B_/
	VP	<isLocatedIn>	1262926
------
rdfs__label$$3$$	0	VP	rdfs__label/
	VP	<rdfs__label>	20236705
------
rdf__type$$9$$	0	VP	rdf__type/
	VP	<rdf__type>	61165359
------
_L_directed_B_$$8$$	0	VP	_L_directed_B_/
	VP	<directed>	41811
------
