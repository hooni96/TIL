# -*- coding: utf-8 -*-
import os, re

os.chdir(r'/Users/jihunmin/TIL/python')

f = open('friends101.txt', 'r', encoding = 'utf-8')
script101 = f.read()

# 지문만 출력하기
Line = re.findall(r'\([A-Za-z].+[a-z|\.]\)', script101)[:6]
print(Line)
# print(Line[:3])
#for item in Line[:3]:
#    print(item)