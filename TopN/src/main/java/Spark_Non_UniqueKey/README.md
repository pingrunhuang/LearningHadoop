## description

we can come across with the situation where the key is not unique like when we want to get the top 2 url that are most viewed.

(url, count)
server 1        | server 2          | server 3
(A,2)               (A,1)               (B,2)
(B,2)               (B,2)               (G,2)
(C,3)               (C,2)               (F,2)
(D,2)               (D,2)               (E,2)
(E,1)
(G,2)

We can not simply get the top 2 URL in each partition and aggregate them locally later like we did for unique key
What we can do is
1. sum them together to get the unique key
2. assign them to M partition
3. get the top 2
4. get the final top 2
