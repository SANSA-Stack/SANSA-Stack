SELECT tab2.a1 AS a1 , tab5.a2 AS a2 , tab4.movie AS movie , tab0.name1 AS name1 , tab1.name2 AS name2 
 FROM    (SELECT sub AS a1 
	 FROM rdf__type$$3$$ 
	 WHERE obj = '<wikicategory_English_film_actors>'
	) tab2
 JOIN    (SELECT sub AS a1 , obj AS name1 
	 FROM skos__prefLabel$$1$$
	
	) tab0
 ON(tab2.a1=tab0.a1)
 JOIN    (SELECT sub AS a1 , obj AS movie 
	 FROM _L_actedIn_B_$$5$$
	) tab4
 ON(tab0.a1=tab4.a1)
 JOIN    (SELECT obj AS movie , sub AS a2 
	 FROM _L_actedIn_B_$$6$$
	) tab5
 ON(tab4.movie=tab5.movie)
 JOIN    (SELECT sub AS a2 
	 FROM rdf__type$$4$$ 
	 WHERE obj = '<wikicategory_English_film_actors>'
	) tab3
 ON(tab5.a2=tab3.a2)
 JOIN    (SELECT sub AS a2 , obj AS name2 
	 FROM skos__prefLabel$$2$$
	
	) tab1
 ON(tab3.a2=tab1.a2)
 
 WHERE (tab2.a1 != tab5.a2)

++++++Tables Statistic
_L_actedIn_B_$$6$$	0	VP	_L_actedIn_B_/
	VP	<actedIn>	127513
	SS	<actedIn><skos__prefLabel>	127513	1.0
	SS	<actedIn><rdf__type>	127513	1.0
------
rdf__type$$3$$	2	SS	rdf__type/_L_actedIn_B_
	VP	<rdf__type>	61165359
	SS	<rdf__type><skos__prefLabel>	61159360	1.0
	SS	<rdf__type><actedIn>	832287	0.01
------
skos__prefLabel$$1$$	2	SS	skos__prefLabel/_L_actedIn_B_
	VP	<skos__prefLabel>	2954875
	SS	<skos__prefLabel><rdf__type>	2886010	0.98
	SS	<skos__prefLabel><actedIn>	26665	0.01
------
skos__prefLabel$$2$$	2	SS	skos__prefLabel/_L_actedIn_B_
	VP	<skos__prefLabel>	2954875
	SS	<skos__prefLabel><rdf__type>	2886010	0.98
	SS	<skos__prefLabel><actedIn>	26665	0.01
------
_L_actedIn_B_$$5$$	0	VP	_L_actedIn_B_/
	VP	<actedIn>	127513
	SS	<actedIn><skos__prefLabel>	127513	1.0
	SS	<actedIn><rdf__type>	127513	1.0
------
rdf__type$$4$$	2	SS	rdf__type/_L_actedIn_B_
	VP	<rdf__type>	61165359
	SS	<rdf__type><skos__prefLabel>	61159360	1.0
	SS	<rdf__type><actedIn>	832287	0.01
------
