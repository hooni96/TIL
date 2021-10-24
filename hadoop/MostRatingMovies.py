# _*_ coding: utf-8 _*_
# 과제1. 평균 평점이 2.0이 안되는 영화 중 가장 평가를 많이 받았던 30개의 영화를 출력하세요.

from pyspark import SparkConf, SparkContext
from itertools import islice
import csv

def loadMovies():
    movies = {} # 맵.. 파이썬에서는 딕셔너리
    # 조인안하려고 movies.csv가 작은거 아니깐.. keep 해두는 건데.. 보통 이렇게 안함
    with open("/home/maria_dev/ml-latest-small/movies.csv", "rb") as f:
        reader = csv.reader(f, delimiter=',')
        next(reader) # skip header
        for row in reader:
            movies[int(row[0])] = row[1] # movieId, title
    return movies

def parseInput(line):
    fields = line.split(',')
    return (int(fields[1]), (float(fields[2]), 1.0))

# 수행되는 부분
if __name__ == "__main__":
    movies = loadMovies()
    path = "hdfs:///user/maria_dev/ml-latest-small/ratings.csv"

    # create spark context
    conf = SparkConf().setAppName("MostRatingMovies")
    sc = SparkContext(conf = conf) # sc밑에 spark operater 있음!

    # create RDD from text file
    lines = sc.textFile(path)
    
    # skip header
    lines = lines.mapPartitionsWithIndex(
        lambda idx, it: islice(it, 1, None) if idx == 0 else it
    )
    
    # line을 ,로 나눠서 (movieId, (ratings, 1.0)) emit
    # RDD가 계속 만들어 지고 있는 중
    ratings = lines.map(parseInput)

    # reduce to (movieId, (sumOfRating, countRating)) 즉 rating 합과 개수를 뱉어냄.
    # 다음 결과에 메모리 되서 들어가는.. S = S + a 라고 생각하면 됨
    sumAndCounts = ratings.reduceByKey(lambda m1, m2: (m1[0] +m2[0], m1[1] +m2[1]))

    # sumAndCount --> (movieId, (averageRating, countRating)) 평점 구하기!
    avgRatings = sumAndCounts.mapValues(lambda v: (v[0] / v[1], v[1]))
    
    # filter를 걸어서 평균이 2.0 안되는 녀석들만 걸러준다.
    filterRatings = avgRatings.filter(lambda x: x[1][0] < 2.0)
    
    # sort
    sortedMovies = filterRatings.sortBy(lambda x: x[1][1], ascending=False)

    # top 10 / take에서 action
    results = sortedMovies.take(30)
    
    # reduceByKey랑 take에서 action이 일어난다. 실질적이 계산이 이뤄짐.
    
    for result in results:
        print(movies[result[0]], result[1][0], result[1][1])