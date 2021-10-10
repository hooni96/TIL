# _*_ coding: utf-8 _*_
# 가장 별점을 많이 받은 영화는?

from mrjob.job import MRJob
from mrjob.step import MRStep

class PopularMovie(MRJob):
    def steps(self):
        return [
            # 영화 별 별점 개수 구하기
            MRStep(mapper = self.map_rating_count,
                    combiner = self.combine_rating_count,
                    reducer = self.reduce_rating_count),
            # 별점 개수를 이용한 Sort
            MRStep(reducer=self.reduce_sort)
        ]

    def map_rating_count(self, _, line):
        data = line.split(',')
        if data[0] != 'userId':
            yield data[1], 1 # movieId, 1 쌍으로 뽑아낸다.
    
    def combine_rating_count(self, movie_id, count):
        # mapping 결과를 reduce에 넣어 주기전 한 번 reduce해서 속도를 향상 시킴
        yield movie_id, sum(count)

    def reduce_rating_count(self, movie_id, counts):
            yield str(sum(counts)).zfill(6), movie_id
            # key는 전부 String으로 본다. Sorting될 때 오류 안나도록 6자리 0으로 채워줌
    
    def reduce_sort(self, count, movie_ids):
        # sort가 key순서대로 되는 점을 이용해 count 순으로 정렬함
        for movie in movie_ids:
            yield movie, count

if __name__ == '__main__':
    PopularMovie.run()