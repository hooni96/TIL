# _*_ coding: utf-8 _*_
# 과제2. 평균 평점이 2.0이 안되는 영화 중 가장 평가를 많이 받았던 30개의 영화를 출력하세요.
# 단 Spark SQL 이용!

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("WorstMovies").getOrCreate()
    
    # inferSchema = "true" 파일의 첫 줄을 스키마로 사용하겠다.
    df1 = spark.read.load("hdfs:///user/maria_dev/ml-latest-small/ratings.csv",
                          format="csv", sep=",", inferSchema="true", header="true")

    df2 = spark.read.load("hdfs:///user/maria_dev/ml-latest-small/movies.csv",
                          format="csv", sep=",", inferSchema="true", header="true")
    
    # sql에 이용하려고 table 이름 주기~!
    df1.createOrReplaceTempView("ratings")
    df2.createOrReplaceTempView("movies")
    
    # SQL 이용해서 손쉽게 원하는 값 projection!
    result = spark.sql("""
        SELECT title, score
        FROM movies JOIN(
            SELECT movieId, avg(rating) as score
            FROM ratings GROUP BY movieId
        ) r ON movies.movieId = r.movieId
        WHERE score < 2.0
        ORDER BY score DESC LIMIT 30
        """)

    for row in result.collect():
        print(row.title, row.score)

