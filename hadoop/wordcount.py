# _*_ coding: utf-8 _*_

from mrjob.job import MRJob
import re	# 워드 자르려고

WORD_RE = re.compile(r"[\w']+")

class MRWordFreqCount(MRJob):
	# 이 MRJob에 함수 define 되어있어서 overriding해서 custom해주면 된다.
	def mapper(self, _, line):
		for word in WORD_RE.findall(line):
			yield(word.lower(), 1)

	def combiner(self, word, counts):
    	# 1차 reducing해주면 성능 올라간다!
		yield(word, sum(counts))

	def reducer(self, word, counts):
		yield(word, sum(counts))

if __name__ == '__main__':
	MRWordFreqCount.run()