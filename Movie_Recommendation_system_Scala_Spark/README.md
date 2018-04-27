# Description:
'Model_Based_Colaborative_Filtering.scala' uses ALS library to build a recommendation systems.

# How to run:
spark-submit --class main task1V1.jar ratings.csv testing_small.csv

# Results:
>=0 and <1: 12227
>=1 and <2: 5715
>=2 and <3: 727
>=3 and <4: 59
>=4: 5
Mean Squared Error = 1.0259116930548453
Elapsed time: 6.256974426s


# Description:
'User_Based_Collaborative_Filtering.py' implements item based collaborative filtering using pyspark library. To speed up the process, only similar items rated by the same user are used. Values exceeding 5 or less than zero are mapped back to 5 and zero respectively.

# Results:
The accuracy and time information are as follows:

>=0 and <1: 14380

>=1 and <2: 4620

>=2 and <3: 1033

>=3 and <4: 206

>=4: 17

RMSE= 1.0016661895

Total time passed in seconds: 242.24000001 seconds
