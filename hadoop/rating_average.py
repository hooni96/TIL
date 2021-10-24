# _*_ coding: utf-8 _*_
# 각 영화별 평균 평점을 계산하는 MapReduce 프로그램

from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import defaultdict
import csv

class MovieRatingAverage(MRJob):
    def steps(self):
        return [
            # 영화별 평균 평점 계산
            MRStep(mapper = self.map_rating,
                    reducer = self.reduce_avg_rating),
            # 평균평점을 이용한 Sort
            MRStep(reducer=self.reduce_sort)
        ]

    # csv패키지를 사용하여 line으로 가져옴
    def map_rating(self, _, line):
        splits = next(csv.reader([line]))
        # 첫 줄은 column 이니 버리고,
        if splits[0] != 'userId' and splits[0] != 'movieId':
            # splits의 길이로, 즉 column이 몇 개인지로 구분해서 ratings와 movies에서 필요한 데이터 emit
            if len(splits) == 4:
                yield splits[1], (float(splits[2]), 1) # movieId, (rating, count)
            else:
                yield splits[0], splits[1] # movieId, title
    
    def reduce_avg_rating(self, movie_id, values):
        # rating, title 담을 dictionary를 선언한다.
        dict_ratings = defaultdict(list)
        dict_title = defaultdict(str)

        for value in values:
            if type(value) == list:
                # type이 list면 (rating, count)를 value로 넣고
                dict_ratings[movie_id].append(value)
            else:
                # 아니면 title를 value로 넣는다.
                dict_title[movie_id] = value.encode('utf-8')

        for k,v in dict_ratings.items():
            # rating dictionary를 루프를 돌려, rating, count까리 더한다.
            sum_ratings = sum(int(i) for i,j in v)
            sum_counts = sum(int(j) for i,j in v)
            # 리뷰 회수가 10회 이하인 리뷰는 포함하지 않기로 하였기에, 10초과 일때만 emit
            if sum_counts > 10:
                # sum_ratings은 실수 일 가능성이 그므로 sum_counts에 실수를 곱해주고
                # 보기 깔끔하게 formating과 반올림을 해줬다. 평균폄점과 영화제목을 emit
                yield "%03.2f"%round(sum_ratings/(sum_counts*1.0),2), dict_title[k] 
    
    def reduce_sort(self, rating, title):
        # sort가 key순서대로 되는 점을 이용해 평균평점 순으로 정렬함
        for movie in title:
            yield movie, float(rating)

if __name__ == '__main__':
    MovieRatingAverage.run()