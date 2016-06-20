SELECT tab2.individual AS individual , tab4.name AS name , tab5.prize AS prize , tab3.musician AS musician 
 FROM    (SELECT sub AS musician 
	 FROM skos__prefLabel$$4$$
	 
	 WHERE obj = '"musician"@eng'
	) tab3
 JOIN    (SELECT sub AS individual , obj AS musician 
	 FROM rdf__type$$3$$
	) tab2
 ON(tab3.musician=tab2.musician)
 JOIN    (SELECT sub AS individual 
	 FROM rdf__type$$1$$ 
	 WHERE obj = '<wikicategory_American_male_singers>'
	) tab0
 ON(tab2.individual=tab0.individual)
 JOIN    (SELECT sub AS individual 
	 FROM rdf__type$$2$$ 
	 WHERE obj = '<wikicategory_Roadrunner_Records_artists>'
	) tab1
 ON(tab0.individual=tab1.individual)
 JOIN    (SELECT sub AS individual , obj AS prize 
	 FROM _L_hasWonPrize_B_$$6$$
	
	) tab5
 ON(tab1.individual=tab5.individual)
 JOIN    (SELECT sub AS individual , obj AS name 
	 FROM skos__prefLabel$$5$$
	
	) tab4
 ON(tab5.individual=tab4.individual)


++++++Tables Statistic
skos__prefLabel$$4$$	0	VP	skos__prefLabel/
	VP	<skos__prefLabel>	2954875
------
skos__prefLabel$$5$$	0	VP	skos__prefLabel/
	VP	<skos__prefLabel>	2954875
------
rdf__type$$2$$	0	VP	rdf__type/
	VP	<rdf__type>	61165359
------
rdf__type$$1$$	0	VP	rdf__type/
	VP	<rdf__type>	61165359
------
rdf__type$$3$$	0	VP	rdf__type/
	VP	<rdf__type>	61165359
------
_L_hasWonPrize_B_$$6$$	0	VP	_L_hasWonPrize_B_/
	VP	<hasWonPrize>	73763
------
