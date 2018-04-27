# Description
The code finds frequent item-sets using SON algorithm. The candidate frequent item-sets are extracted per worker using a Priori algorithm and then map-reduce features are used to find the actual frequent item-sets.

# Command line instructions
Command line usage is as follows:
1: The first parameter is the case number and can be either 1 or 2
2: Second parameter is the path of ratings file; here I assume the file is in the same folder as the code
3: Third parameter is the path of ratings file;  here I assume the file is in the same folder as the code
4: The results will be written in the files as instructed in the same folder as the code

# Usage
spark-submit SON_pySpark.py 1 ratings.dat users.dat 