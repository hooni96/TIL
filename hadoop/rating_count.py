# _*_ coding: utf-8 _*_
# 사람들이 가장 많이주는 별점은?

from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper = self.map_rating_count,
                    reducer = self.reduce_rating_count)
        ]
    # function 이름 강제로 준 것, 상속받아서 사용해도 무방!!
    def map_rating_count(self, _, line):
        # 한 줄씩 받아서 ',' 기준으로 나눈다.
        data = line.split(',')
        if data[0] != 'userId': # rating.csv 파일 첫 줄은 column name이니
            yield data[2], 1    # rating만 뽑아낸다.
    
    def reduce_rating_count(self, key, values):
        yield key, sum(values)  # rating별 횟수를 모두 더한다.

if __name__ == '__main__':
    RatingCount.run()