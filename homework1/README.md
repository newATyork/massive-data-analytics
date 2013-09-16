I ran the JAR in these forms as follows to get the outputs:


Job a: specify certain file

bin/hadoop  jar  hw1.jar  /user/hduser/addr/addresses.csv  /user/hduser/hw1a/  A       



Job b:specify certain file

bin/hadoop  jar  hw1.jar  /user/hduser/addr/addresses.csv  /user/hduser/hw1b/  B



Job c:take all files in the folder 

bin/hadoop  jar  hw1.jar  /user/hduser/addr/  /user/hduser/hw1c/  C

==============================================================================

http://www.vistrails.org/index.php/BigDataHW1

BigDataHW1
cs9223 - Massive Data Analysis - Homework Assignment 1
Hands-on experience with Hadoop/MapReduce

As a good citizen of NYC, you want to be prepared for potential disaster scenarios and you want to learn about hurricane evacuation centers in NYC (https://www.google.com/fusiontables/DataSource?docid=1BiOUN5JP94FT5pV9UNCQIvFloHqS_NTboOTvVD8). You will:
Install Hadoop and HDFS on your laptop
The hurricane center data comes in two files:
File1 (http://vgc.poly.edu/~juliana/courses/cs9223/Homework/HW1/hurricane-center-addresses.csv) contains the addresses for hurricane centers
File2 ("http://vgc.poly.edu/~juliana/courses/cs9223/Homework/HW1/hurricane-center-coordinates.csv") contains the coordinates for the centers
Write and run MapReduce programs that:
a)	Select the tuples from File1 where zipcode<10030
b)	Eliminate duplicates from File1
c)	Compute the natural join between File1 and File2, i.e., create a new file that contains both the addresses and coordinates for the hurricane centers
You should submit your java program source and binary (named hw1.java and hw1.jar). The program should accept as an argument the question letter (a,b, and c) and output the answer for that question. Your program should read the files from their http location.



