##Crystal Ball to Predict Events

In this project you will create a crystal ball to predict events that may happen once a certain event happened.
 

Example: Amazon will say people who bought “item one” have bought the following items : “item two”, “item three”, “item four”.
 

For the purpose of this project you can assume that historical customer data is available in the following form.

 

18 34 56 29 12 34 56 92 29 34 12      // items bought by a customer, listed in the order she bought it

92 29 18 12 34 79 29 56 12 34 18  // items bought by another customer, listed in the order she bought it

…

 

Let the neighborhood of X, N(X) be set of all term after X and before the next X.

Example: Let Data block be [a b c a d e]

N(a) = {b, c}, N(b) = {c, a, d, e}, N(c) = {a, d, e}, N(a) ={d, e}, N(d) = {e}, N(e) = {}.

 
 

Part 1. [5 point]  Set up a single node cluster and optionally an eclipse development environment to create and test your programs.

(a) Get VMWare or VirtualBox (install)

(b) Get Cloudera (install)

(c) Get WordCount (test run)

Document all the steps and submit. It should be as detailed as the sample document attached.

If the detailed document is not there, the credit will be 0 for Part 1.

Part 2. Implement Pairs algorithm to compute relative frequencies.

[2 points] Create Java classes (.java files)
[1 points] Show input, output and batch file to execute your program at command line in Hadoop.
Part 3. Implement Stripes algorithm to compute relative frequencies.

[2 points] Create Java classes (.java files)
[1 points] Show input, output and batch file to execute your program at command line in Hadoop.
Part 4. Implement Pairs in Mapper and Stripes in Reducer to compute relative frequencies.

[2 points] Create Java classes (.java files)
[1 points] Show input, output and batch file to execute your program at command line in Hadoop.