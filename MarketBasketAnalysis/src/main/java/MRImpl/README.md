### Problem description

Dataset [groceries.csv](http://www.salemmarafi.com/wp-content/uploads/2014/03/groceries.csv)

Notice:
we should ignore the sequence of the data item. For example, [\<crackers>,\<icecream>,1] should be the same as
[\<icecream>,\<crackers>,1] which lead to our next concern -- we should sort each transaction at first.
     

### Target

get the support for each combination of different items of a transaction.

[\<icecream>,\<crackers>,2]
