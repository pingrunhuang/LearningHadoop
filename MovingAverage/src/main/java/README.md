# Definition
int[] a = new int[N]
we can define n moving average as a list of S

S[n] = {(a[0] + a[1] + ... + a[n-1])/n, (a[1] + a[2] + ... + a[i+1])/n, ... , 
        (a[N-n+1] + a[N-n] + a[N-n-1] + ... + a[N])/n}
         
 