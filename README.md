# MapReduceMrJob-
Perform a few exercises for Map Reduce with Mrjob

Do All this homework with this source :

https://mrjob.readthedocs.io/en/latest/

 # Mrjob
 
 mrjob is the easiest route to writing Python programs that run on Hadoop. If you use mrjob, you’ll be able to test your code locally without installing Hadoop or run it on a cluster of your choice.

Additionally, mrjob has extensive integration with Amazon Elastic MapReduce. Once you’re set up, it’s as easy to run your job in the cloud as it is to run it on your laptop.

Here are a number of features of mrjob that make writing MapReduce jobs easier:

Keep all MapReduce code for one job in a single class
Easily upload and install code and data dependencies at runtime
Switch input and output formats with a single line of code
Automatically download and parse error logs for Python tracebacks
Put command line filters before or after your Python code
If you don’t want to be a Hadoop expert but need the computing power of MapReduce, mrjob might be just the thing for you.


 # Tamrine 1
 
 Find average of column in dataset
 
 # Tamrine 2 
 
 Find Standard deviation on columns
 
 # Tamrine 3 
 
 Find Variance on columns
 
 # Tamrine 4
 
 Multiply 2 matrices
 


Exerci
ese for Section 2.3 (Mining of Massive Datasets)
Exercise 2.3.1 :

Design map-reduce algorithms to take a very large file of integers and produce as output:
(a) The largest integer.
(b) The average of all the integers.
(c) The same set of integers, but with each integer appearing only once.
(d) The count of the number of distinct integers in the input.
solution (a)

Map : for each inteager i in the file , emit key-value pair(m,i)
Reduce:input of this task is pair thet this key is m ,and associated value is lis[i1,...,in].that each ij is value of pair of output of map task.output of reduce task is pair(m,max{i1,...,in})
notice that max is a associative and commutative function,we can produce only one pair (m,max{i1,...,in}) in map task.
solution (b)

Map : for each inteager i in the file , emit key-value pair(m,i)
Reduce:input of this task is pair thet this key is m,and associated value is list[i1,...,in] that each ij is value of pair of output of map task.output of reduce task is pair(1,averge{i1,..,in})
solution (c)

Map : for each inteager i in the file , emit key-value pair(i,i)
Reduce:input of this task is pair thet this key is i,and associated value is lis[i,..,i].that each i is value of pair of output of map task. output of reduce task is pair(i,i)
it produces exactly one pair (i,i) for this key i.
The second method: Map: for each inteager i in the file,emite key-value pair(i,1)
Reduce: turn the value list into 1.
note the result is obtain from the keys of the output.
solution (d)

we sould use two map-reduse
1: Map : for each inteager i in the file , emit key-value pair(i,i)
Reduce:input of this task is pair thet this key is i,and associated value is lis[i,..,i].that each i is value of pair of output of map task. output of reduce task is pair(i,i)
it produces exactly one pair (i,i) for this key i.
2: Map : for each inteager i in the file , emit key-value pair(1,1)
Reduce : input of this task is pair thet this key is 1 ,and associated value is lis[1,..,1].that each 1 is value of pair of output of map task.output of reduce task is pair(1,sum{1,...,1}).output of reduce task is number of distinct integers in the input.
Exercise 2.3.2 :

Our formulation of matrix-vector multiplication assumed that the matrix M was square. Generalize the algorithm to the case where M is an r-by-c matrix for some number of rows r and columns c.
solution

The matrix M and the vector v each will be stored in a file of the DFS. We assume that the row-column coordinates of each matrix element will be discoverable, either from its position in the file, or because it is stored with explicit coordinates, as a triple (i, j,mij). We also assume the position of element vj in the vector v will be discoverable in the analogous way.
The Map Function: Each Map task will take the entire vector v and a chunk of the matrix M. From each matrix element mij it produces the key-value pair (i,mijvj). Thus, all terms of the sum that make up the component xi of the matrix-vector product will get the same key.
The Reduce Function: A Reduce task has simply to sum all the values associated with a given key i. The result will be a pair (i, xi)
If the Vector v Cannot Fit in Main Memory
we can divide the matrix into vertical stripes of equal width and divide the vector into an equal number of horizontal stripes, of the same height. Our goal is to use enough stripes so that the portion of the vector in one stripe can fit conveniently into main memory at a compute node.
The ith stripe of the matrix multiplies only components from the ith stripe of the vector. Thus, we can divide the matrix into one file for each stripe, and do the same for the vector. Each Map task is assigned a chunk from one of the stripes of the matrix and gets the entire corresponding stripe of the vector. The Map and Reduce tasks can then act exactly as was described above for the case where Map tasks get the entire vector.
! Exercise 2.3.3 :

In the form of relational algebra implemented in SQL, relations are not sets, but bags; that is, tuples are allowed to appear more than once. There are extended definitions of union, intersection, and difference for bags, which we shall define below. Write map-reduce algorithms for computing the following operations on bags R and S:
(a) Bag Union, defined to be the bag of tuples in which tuple t appears the sum of the numbers of times it appears in R and S.
(b) Bag Intersection, defined to be the bag of tuples in which tuple t appears the minimum of the numbers of times it appears in R and S.
(c) Bag Difference, defined to be the bag of tuples in which the number of times a tuple t appears is equal to the number of times it appears in R minus the number of times it appears in S. A tuple that appears more times in S than in R does not appear in the difference.
solution (a)

Map: for each tuple t in R and S emit key-value pair(t,t)
Reduce: input: (t,t) output : (t,t)
solution (b)

1: Map: for each tuple t in R emit key-value pair((t,R),1) and for each tuple t in S emit key-value pair((t,S),1)
Reduce : input:((t,R),[1,..,1])or((t,S),[1,..,1]) output: ((t,R),sum[1,..,1]=m)or((t,S),sum[1,..,1]=n)
2: Map:input: ((t,R),m) or ((t,S),n) output:(t,m)or(t,n)
Reduce: input:(t,[m,n]) output:(t,min(m,n)=d)
3: Map: input: (t,d) output:(t,d)
Reduce: input:(t,d) output:produce d tuple(t,t).
solution (c)

1: Map: for each tuple t in R emit key-value pair((t,R),1) and for each tuple t in S emit key-value pair((t,S),1)
Reduce : input:((t,R),[1,..,1])or((t,S),[1,..,1]) output: ((t,R),sum[1,..,1]=m)or((t,S),sum[1,..,1]=n)
2: Map:input: ((t,R),m) or ((t,S),n) output:(t,m)or(t,n)
Reduce: input:(t,[m,n]) output:(t,m-n=d)
3: Map: input: (t,d) output:(t,d)
Reduce: input : (t,d) output : produce d tuple(t,t).(if d=<0,prodce nothing)
! Exercise 2.3.4:

Selection can also be performed on bags. Give a map-reduce implementation that produces the proper number of copies of each tuple t that passes the selection condition. That is, produce key-value pairs from which the correct result of the selection can be obtained easily from the values.
solution

Map: for each tuple t stisfy C output:(t,1)
Reduce: input:(t,[1,..,1]) output: (t,sum(1,..,1))
Exercise 2.3.5 :

The relational-algebra operation R(A,B) ⊲⊳ B<C S(C,D) produces all tuples (a, b, c, d) such that tuple (a, b) is in relation R, tuple (c, d) is in S, and b < c. Give a map-reduce implementation of this operation, assuming R and S are sets.
solution

1:Map: For each tuple (a, b) of R, produce the key-value pair (1,((R,b) , (R, a))). For each tuple (c,d) of S, produce the key-value pair (1,((S,c),(S,d)))
Reduce:input: (1,[((R,b),(R, a)),....,((S,c),(S,d)),...] output: Construct all pairs consisting of one with first component R and the other with first component S, The output for key (b,c) is ((b,c),((R,a),(S,d))
2:Map: input:((b,c),((R,a),(S,d)) outpt: (if b<c ) ((b,c),((R,a),(S,d))
Reduce: input: ((b,c),((R,a),(S,d)) output: (a,b,c,d).
