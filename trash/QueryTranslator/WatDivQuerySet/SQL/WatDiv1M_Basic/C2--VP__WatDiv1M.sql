SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab4.v5 AS v5 , tab7.v7 AS v7 , tab5.v6 AS v6 , tab6.v4 AS v4 , tab9.v9 AS v9 , tab3.v3 AS v3 , tab8.v8 AS v8 , tab2.v2 AS v2 
 FROM    (SELECT sub AS v2 
	 FROM sorg__eligibleRegion$$3$$
	 
	 WHERE obj = 'wsdbm:Country5'
	) tab2
 JOIN    (SELECT obj AS v3 , sub AS v2 
	 FROM gr__includes$$4$$
	) tab3
 ON(tab2.v2=tab3.v2)
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM gr__offers$$2$$
	) tab1
 ON(tab3.v2=tab1.v2)
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM sorg__legalName$$1$$
	
	) tab0
 ON(tab1.v0=tab0.v0)
 JOIN    (SELECT sub AS v3 , obj AS v8 
	 FROM rev__hasReview$$9$$
	
	) tab8
 ON(tab3.v3=tab8.v3)
 JOIN    (SELECT obj AS v9 , sub AS v8 
	 FROM rev__totalVotes$$10$$
	
	) tab9
 ON(tab8.v8=tab9.v8)
 JOIN    (SELECT sub AS v7 , obj AS v3 
	 FROM wsdbm__purchaseFor$$8$$
	
	) tab7
 ON(tab8.v3=tab7.v3)
 JOIN    (SELECT obj AS v7 , sub AS v4 
	 FROM wsdbm__makesPurchase$$7$$
	
	) tab6
 ON(tab7.v7=tab6.v7)
 JOIN    (SELECT obj AS v5 , sub AS v4 
	 FROM sorg__jobTitle$$5$$
	
	) tab4
 ON(tab6.v4=tab4.v4)
 JOIN    (SELECT obj AS v6 , sub AS v4 
	 FROM foaf__homepage$$6$$
	
	) tab5
 ON(tab4.v4=tab5.v4)


++++++Tables Statistic
gr__includes$$4$$	0	VP	gr__includes/
	VP	<gr__includes>	9000
------
foaf__homepage$$6$$	0	VP	foaf__homepage/
	VP	<foaf__homepage>	1068
------
rev__hasReview$$9$$	0	VP	rev__hasReview/
	VP	<rev__hasReview>	14757
------
gr__offers$$2$$	0	VP	gr__offers/
	VP	<gr__offers>	14179
------
sorg__legalName$$1$$	0	VP	sorg__legalName/
	VP	<sorg__legalName>	13
------
wsdbm__purchaseFor$$8$$	0	VP	wsdbm__purchaseFor/
	VP	<wsdbm__purchaseFor>	15000
------
sorg__jobTitle$$5$$	0	VP	sorg__jobTitle/
	VP	<sorg__jobTitle>	500
------
wsdbm__makesPurchase$$7$$	0	VP	wsdbm__makesPurchase/
	VP	<wsdbm__makesPurchase>	15000
------
rev__totalVotes$$10$$	0	VP	rev__totalVotes/
	VP	<rev__totalVotes>	766
------
sorg__eligibleRegion$$3$$	0	VP	sorg__eligibleRegion/
	VP	<sorg__eligibleRegion>	22655
------
