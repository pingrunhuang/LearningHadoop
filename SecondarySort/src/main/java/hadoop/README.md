## Secondary sort based on hadoop mapreduce framework

(K, V1), (K, V2), (K,V3) ... (K, Vn)
with Vi is a tuple with m properties eg.
(Ai1, Ai2, ... Aim)

target:
we want to reduce by the Ai1 so that (K,(A11,r1)),(K,(A21,r2))... where r=(Ai2,Ai3,...Aim)

(K,A11) --> (A11,(A12,A13,...))
where
K is natural key
(K,A11) is composite key
(A11,(A12,A13,...)) is natural value

which means part of the composite key is also the one in the natural value that we are going to sort
in this case, A11 is not necessarily to be shown in the result.