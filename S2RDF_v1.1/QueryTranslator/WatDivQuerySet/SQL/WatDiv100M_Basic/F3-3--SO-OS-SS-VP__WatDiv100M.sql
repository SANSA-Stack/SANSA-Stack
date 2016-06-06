SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab5.v5 AS v5 , tab4.v6 AS v6 , tab3.v4 AS v4 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM wsdbm__hasGenre$$3$$
	 
	 WHERE obj = 'wsdbm:SubGenre124'
	) tab2
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM sorg__contentRating$$1$$
	
	) tab0
 ON(tab2.v0=tab0.v0)
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM sorg__contentSize$$2$$
	
	) tab1
 ON(tab0.v0=tab1.v0)
 JOIN    (SELECT obj AS v0 , sub AS v5 
	 FROM wsdbm__purchaseFor$$6$$
	
	) tab5
 ON(tab1.v0=tab5.v0)
 JOIN    (SELECT obj AS v5 , sub AS v4 
	 FROM wsdbm__makesPurchase$$4$$
	
	) tab3
 ON(tab5.v5=tab3.v5)
 JOIN    (SELECT sub AS v5 , obj AS v6 
	 FROM wsdbm__purchaseDate$$5$$
	
	) tab4
 ON(tab3.v5=tab4.v5)


++++++Tables Statistic
wsdbm__purchaseDate$$5$$	1	SO	wsdbm__purchaseDate/wsdbm__makesPurchase
	VP	<wsdbm__purchaseDate>	450249
	SO	<wsdbm__purchaseDate><wsdbm__makesPurchase>	450245	1.0
	SS	<wsdbm__purchaseDate><wsdbm__purchaseFor>	450249	1.0
------
wsdbm__purchaseFor$$6$$	2	OS	wsdbm__purchaseFor/sorg__contentSize
	VP	<wsdbm__purchaseFor>	1500000
	OS	<wsdbm__purchaseFor><sorg__contentRating>	423972	0.28
	OS	<wsdbm__purchaseFor><sorg__contentSize>	143927	0.1
	OS	<wsdbm__purchaseFor><wsdbm__hasGenre>	1500000	1.0
	SO	<wsdbm__purchaseFor><wsdbm__makesPurchase>	1499988	1.0
	SS	<wsdbm__purchaseFor><wsdbm__purchaseDate>	450249	0.3
------
wsdbm__hasGenre$$3$$	2	SS	wsdbm__hasGenre/sorg__contentSize
	VP	<wsdbm__hasGenre>	593924
	SS	<wsdbm__hasGenre><sorg__contentRating>	179575	0.3
	SS	<wsdbm__hasGenre><sorg__contentSize>	59093	0.1
	SO	<wsdbm__hasGenre><wsdbm__purchaseFor>	386896	0.65
------
sorg__contentSize$$2$$	1	SS	sorg__contentSize/sorg__contentRating
	VP	<sorg__contentSize>	24860
	SS	<sorg__contentSize><sorg__contentRating>	7468	0.3
	SS	<sorg__contentSize><wsdbm__hasGenre>	24860	1.0
	SO	<sorg__contentSize><wsdbm__purchaseFor>	16204	0.65
------
sorg__contentRating$$1$$	1	SS	sorg__contentRating/sorg__contentSize
	VP	<sorg__contentRating>	75412
	SS	<sorg__contentRating><sorg__contentSize>	7468	0.1
	SS	<sorg__contentRating><wsdbm__hasGenre>	75412	1.0
	SO	<sorg__contentRating><wsdbm__purchaseFor>	49206	0.65
------
wsdbm__makesPurchase$$4$$	1	OS	wsdbm__makesPurchase/wsdbm__purchaseDate
	VP	<wsdbm__makesPurchase>	1499988
	OS	<wsdbm__makesPurchase><wsdbm__purchaseDate>	450245	0.3
	OS	<wsdbm__makesPurchase><wsdbm__purchaseFor>	1499988	1.0
------
