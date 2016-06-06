SELECT tab6.country1 AS country1 , tab2.area AS area , tab4.geo AS geo , tab11.lon2 AS lon2 , tab0.lon1 AS lon1 , tab6.country2 AS country2 , tab9.name AS name , tab8.property AS property , tab3.wiki AS wiki , tab7.capital AS capital , tab5.lang AS lang , tab12.lat2 AS lat2 , tab10.url AS url , tab1.lat1 AS lat1 
 FROM    (SELECT sub AS country1 , obj AS country2 
	 FROM _L_dealsWith_B_$$7$$
	) tab6
 JOIN    (SELECT sub AS country1 , obj AS lang 
	 FROM _L_hasOfficialLanguage_B_$$6$$
	
	) tab5
 ON(tab6.country1=tab5.country1)
 JOIN    (SELECT sub AS country2 , obj AS capital 
	 FROM _L_hasCapital_B_$$8$$
	) tab7
 ON(tab6.country2=tab7.country2)
 JOIN    (SELECT obj AS property , sub AS capital 
	 FROM _L_owns_B_$$9$$
	) tab8
 ON(tab7.capital=tab8.capital)
 JOIN    (SELECT sub AS property , obj AS url 
	 FROM _L_hasWebsite_B_$$11$$
	
	) tab10
 ON(tab8.property=tab10.property)
 JOIN    (SELECT obj AS name , sub AS property 
	 FROM skos__prefLabel$$10$$
	
	) tab9
 ON(tab10.property=tab9.property)
 JOIN    (SELECT sub AS property , obj AS lat2 
	 FROM _L_hasLatitude_B_$$13$$
	
	) tab12
 ON(tab9.property=tab12.property)
 JOIN    (SELECT obj AS lon2 , sub AS property 
	 FROM _L_hasLongitude_B_$$12$$
	
	) tab11
 ON(tab12.property=tab11.property)
 JOIN    (SELECT sub AS geo , obj AS lang 
	 FROM _L_linksTo_B_$$5$$
	) tab4
 ON(tab5.lang=tab4.lang)
 JOIN    (SELECT obj AS area , sub AS geo 
	 FROM _L_hasArea_B_$$3$$
	) tab2
 ON(tab4.geo=tab2.geo)
 JOIN    (SELECT sub AS geo , obj AS wiki 
	 FROM _L_hasWikipediaUrl_B_$$4$$
	
	) tab3
 ON(tab2.geo=tab3.geo)
 JOIN    (SELECT sub AS geo , obj AS lat1 
	 FROM _L_hasLatitude_B_$$2$$
	
	) tab1
 ON(tab3.geo=tab1.geo)
 JOIN    (SELECT sub AS geo , obj AS lon1 
	 FROM _L_hasLongitude_B_$$1$$
	
	) tab0
 ON(tab1.geo=tab0.geo)


++++++Tables Statistic
_L_dealsWith_B_$$7$$	0	VP	_L_dealsWith_B_/
	VP	<dealsWith>	947
------
_L_hasCapital_B_$$8$$	0	VP	_L_hasCapital_B_/
	VP	<hasCapital>	1937
------
_L_hasLongitude_B_$$12$$	0	VP	_L_hasLongitude_B_/
	VP	<hasLongitude>	7554653
------
_L_hasOfficialLanguage_B_$$6$$	0	VP	_L_hasOfficialLanguage_B_/
	VP	<hasOfficialLanguage>	964
------
_L_hasLatitude_B_$$13$$	0	VP	_L_hasLatitude_B_/
	VP	<hasLatitude>	7554186
------
_L_owns_B_$$9$$	0	VP	_L_owns_B_/
	VP	<owns>	26551
------
skos__prefLabel$$10$$	0	VP	skos__prefLabel/
	VP	<skos__prefLabel>	2954875
------
_L_hasLongitude_B_$$1$$	0	VP	_L_hasLongitude_B_/
	VP	<hasLongitude>	7554653
------
_L_hasLatitude_B_$$2$$	0	VP	_L_hasLatitude_B_/
	VP	<hasLatitude>	7554186
------
_L_hasArea_B_$$3$$	0	VP	_L_hasArea_B_/
	VP	<hasArea>	129920
------
_L_linksTo_B_$$5$$	0	VP	_L_linksTo_B_/
	VP	<linksTo>	38048450
------
_L_hasWikipediaUrl_B_$$4$$	0	VP	_L_hasWikipediaUrl_B_/
	VP	<hasWikipediaUrl>	2886878
------
_L_hasWebsite_B_$$11$$	0	VP	_L_hasWebsite_B_/
	VP	<hasWebsite>	226393
------
