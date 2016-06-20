SELECT tab0.v1 AS v1 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM wsdbm__friendOf$$1$$
	 
	 WHERE sub = 'wsdbm:User36840'
	) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM wsdbm__likes$$2$$
	) tab1
 ON(tab0.v1=tab1.v1)
 JOIN    (SELECT obj AS v3 , sub AS v2 
	 FROM sorg__author$$3$$
	) tab2
 ON(tab1.v2=tab2.v2)
 JOIN    (SELECT obj AS v4 , sub AS v3 
	 FROM wsdbm__makesPurchase$$4$$
	
	) tab3
 ON(tab2.v3=tab3.v3)
 JOIN    (SELECT obj AS v5 , sub AS v4 
	 FROM wsdbm__purchaseFor$$5$$
	
	) tab4
 ON(tab3.v4=tab4.v4)
 JOIN    (SELECT sub AS v5 , obj AS v6 
	 FROM sorg__contentRating$$6$$
	
	) tab5
 ON(tab4.v5=tab5.v5)


++++++Tables Statistic
sorg__author$$3$$	0	VP	sorg__author/
	VP	<sorg__author>	3975
------
wsdbm__friendOf$$1$$	0	VP	wsdbm__friendOf/
	VP	<wsdbm__friendOf>	4491142
------
sorg__contentRating$$6$$	0	VP	sorg__contentRating/
	VP	<sorg__contentRating>	7530
------
wsdbm__likes$$2$$	0	VP	wsdbm__likes/
	VP	<wsdbm__likes>	112401
------
wsdbm__makesPurchase$$4$$	0	VP	wsdbm__makesPurchase/
	VP	<wsdbm__makesPurchase>	149998
------
wsdbm__purchaseFor$$5$$	0	VP	wsdbm__purchaseFor/
	VP	<wsdbm__purchaseFor>	150000
------
