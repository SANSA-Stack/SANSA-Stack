SELECT tab0.name AS name , tab7.a AS a , tab3.m1 AS m1 , tab4.movie AS movie , tab9.m2 AS m2 , tab1.actor AS actor 
 FROM    (SELECT sub AS m2 
	 FROM _L_isLocatedIn_B_$$10$$
	 
	 WHERE obj = '<United_States>'
	) tab9
 JOIN    (SELECT sub AS a , obj AS m2 
	 FROM _L_directed_B_$$8$$
	) tab7
 ON(tab9.m2=tab7.m2)
 JOIN    (SELECT obj AS name , sub AS a 
	 FROM skos__prefLabel$$1$$
	
	) tab0
 ON(tab7.a=tab0.a)
 JOIN    (SELECT sub AS a , obj AS m1 
	 FROM _L_actedIn_B_$$4$$
	) tab3
 ON(tab0.a=tab3.a)
 JOIN    (SELECT sub AS m1 
	 FROM _L_isLocatedIn_B_$$7$$
	 
	 WHERE obj = '<Germany>'
	) tab6
 ON(tab3.m1=tab6.m1)
 JOIN    (SELECT sub AS a , obj AS actor 
	 FROM rdf__type$$2$$
	) tab1
 ON(tab3.a=tab1.a)
 JOIN    (SELECT sub AS actor 
	 FROM rdfs__label$$3$$ 
	 WHERE obj = '"actor"@eng'
	) tab2
 ON(tab1.actor=tab2.actor)
 JOIN    (SELECT sub AS m1 , obj AS movie 
	 FROM rdf__type$$5$$
	) tab4
 ON(tab6.m1=tab4.m1)
 JOIN    (SELECT sub AS movie 
	 FROM rdfs__label$$6$$ 
	 WHERE obj = '"movie"@eng'
	) tab5
 ON(tab4.movie=tab5.movie)
 JOIN    (SELECT obj AS movie , sub AS m2 
	 FROM rdf__type$$9$$
	) tab8
 ON(tab5.movie=tab8.movie AND tab7.m2=tab8.m2)


++++++Tables Statistic
rdf__type$$5$$	1	SO	rdf__type/_L_actedIn_B_
	VP	<rdf__type>	61165359
	SO	<rdf__type><actedIn>	1120810	0.02
	OS	<rdf__type><rdfs__label>	44121234	0.72
	SS	<rdf__type><isLocatedIn>	12567002	0.21
------
rdfs__label$$6$$	1	SO	rdfs__label/rdf__type
	VP	<rdfs__label>	20236705
	SO	<rdfs__label><rdf__type>	291465	0.01
	SO	<rdfs__label><rdf__type>	291465	0.01
------
rdf__type$$2$$	4	SS	rdf__type/_L_directed_B_
	VP	<rdf__type>	61165359
	SS	<rdf__type><skos__prefLabel>	61159360	1.0
	OS	<rdf__type><rdfs__label>	44121234	0.72
	SS	<rdf__type><actedIn>	832287	0.01
	SS	<rdf__type><directed>	359680	0.01
------
_L_actedIn_B_$$4$$	0	VP	_L_actedIn_B_/
	VP	<actedIn>	127513
	SS	<actedIn><skos__prefLabel>	127513	1.0
	SS	<actedIn><rdf__type>	127513	1.0
	OS	<actedIn><rdf__type>	127513	1.0
	OS	<actedIn><isLocatedIn>	38353	0.3
	SS	<actedIn><directed>	16167	0.13
------
_L_isLocatedIn_B_$$10$$	1	SO	_L_isLocatedIn_B_/_L_directed_B_
	VP	<isLocatedIn>	1262926
	SO	<isLocatedIn><directed>	10697	0.01
	SS	<isLocatedIn><rdf__type>	1262926	1.0
------
skos__prefLabel$$1$$	3	SS	skos__prefLabel/_L_directed_B_
	VP	<skos__prefLabel>	2954875
	SS	<skos__prefLabel><rdf__type>	2886010	0.98
	SS	<skos__prefLabel><actedIn>	26665	0.01
	SS	<skos__prefLabel><directed>	11130	0.0
------
_L_isLocatedIn_B_$$7$$	1	SO	_L_isLocatedIn_B_/_L_actedIn_B_
	VP	<isLocatedIn>	1262926
	SO	<isLocatedIn><actedIn>	11488	0.01
	SS	<isLocatedIn><rdf__type>	1262926	1.0
------
rdfs__label$$3$$	1	SO	rdfs__label/rdf__type
	VP	<rdfs__label>	20236705
	SO	<rdfs__label><rdf__type>	291465	0.01
------
rdf__type$$9$$	2	SO	rdf__type/_L_directed_B_
	VP	<rdf__type>	61165359
	OS	<rdf__type><rdfs__label>	44121234	0.72
	SO	<rdf__type><directed>	1128635	0.02
	SS	<rdf__type><isLocatedIn>	12567002	0.21
------
_L_directed_B_$$8$$	0	VP	_L_directed_B_/
	VP	<directed>	41811
	SS	<directed><skos__prefLabel>	41811	1.0
	SS	<directed><rdf__type>	41811	1.0
	SS	<directed><actedIn>	5855	0.14
	OS	<directed><rdf__type>	41811	1.0
	OS	<directed><isLocatedIn>	11234	0.27
------
