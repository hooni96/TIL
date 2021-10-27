# _*_ coding: utf-8 _*_
# 실습3. 스파크 MLlib 활용한 collaborative filtering 구현
from pyspark.ml import recommendation
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS # Alternating Least Squares
from pyspark.sql import Row
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName("CollavorativeFilterinig").getOrCreate()

ratings = spark.read.load("hdfs:///user/maria_dev/ml-latest-small/ratings.csv",
                          format="csv", sep=",", inferSchma="true", header="true")

movies = spark.read.load("hdfs:///user/maria_dev/ml-latest-small/movies.csv",
                          format="csv", sep=",", inferSchma="true", header="true")

movies.createOrReplaceTempView("movies")

(training, test) = ratings.randomSplit([0.8, 0.2])
# 자기가 학습한 데이터로 테스트하면 의미가 있나? 그래서 20% 떼어놓고 시작!

# Build the recommendation model
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId",
          ratingCol="rating", coldStartStrategy="drop")

####################################################
# Example1: Validation
model = als.fit(training)

# Evaluate the model
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = "+str(rmse))

####################################################
# Example2: Recommend movies for a new user

# Add New User 0!
# The format of each line is (userID, MovieId, rating, timestamp)
newData = [
    (0,260,4,0), # Star Wars (1977)
    (0,1,3,0), # Toy Story (1995)
    (0,16,3,0), # Casino (1995)
    (0,25,4,0), # Leaving Las Vegas (1995)
    (0,35,4,0), # Twelve Monkeys (a.k.a. 12 Monkeys) (1995)
    (0,335,1,0), # Flintstones, The (1994)
    (0,296,3,0), # Pulp Fiction (1994)
    (0,858,5,0), # Godfather, The (1972)
    (0,50,4,0) # Usual Suspects, The (1995)
]

newUserRating = spark.createDataFrame(newData, ['userId', 'movieId', 'rating', 'timestamp'])
newRatings = ratings.union(newUserRating)
newRatings.createGlobalTempView("ratings")

model = als.fit(newRatings)

user0Ratings = spark.sql("""
    SELECT title, rating
    FROM ratings JOIN movies ON ratings.movieId = movies.movieId
    WHERE userId = 0
    """)

print("User 0 rates ==============================")
for now in user0Ratings.collect():
    print(row)

print("Top Recommendation (rates over 100 times)==============================")

popularMovies = spark.sql("""
    SELECT movieId, 0 as userId
    FROM ratings
    GROUP BY movieId HAVING count(rating) > 100
    """)

recommendations = model.transform(popularMovies)
df = recommendation.join(movies, recommendations['movieId'] == movie['movieId'])
for row in df.sort(df.prediction.desc()).take(10):
    print(row.title, row.prediction)