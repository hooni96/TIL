# _*_ coding: utf-8 _*_
# 실습 1. 평점이 가장 낮은 10개의 영화를 찾아라
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

    # create spart context
    conf = SparkConf().setAppName("WorstMovies")
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

    # reduce to (movieId, (sumOfRating, countRating))
    # 다음 결과에 메모리 되서 들어가는.. S = S + a 라고 생각하면 됨
    # reduceByKey action!
    sumAndCounts = ratings.reduceByKey(lambda m1, m2: (m1[0] +m2[0], m1[1] +m2[1]))

    # sumAndCount --> (movieId, averageRating) 평점
    avgRatings = sumAndCounts.mapValues(lambda v: v[0] / v[1])

    # sort
    sortedMovies = avgRatings.sortBy(lambda x: x[1])

    # top 10 / take에서 action
    results = sortedMovies.take(10)
    
    # reduceByKey랑 take에서 action이 일어난다. 실질적이 계산이 이뤄짐.
    
    for result in results:
        print(movies[result[0]], result[1])