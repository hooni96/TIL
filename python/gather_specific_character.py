# -*- coding: utf-8 -*-
import os, re

os.chdir(r'/Users/jihunmin/TIL/python')

f = open('friends101.txt', 'r', encoding = 'utf-8')
script101 = f.read()

# print(script101[:100])

# 특정 등장인물의 대사만 모으기
Line = re.findall(r'Monica:.+', script101)
# print(Line[:3])
#for item in Line[:3]:
#    print(item)

f.close()
f = open('monica.txt', 'w', encoding = 'utf8')
monica = ''
for i in Line:
    monica += i + '\n'

f.write(monica)
f.close()