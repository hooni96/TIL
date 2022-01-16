# -*- coding: utf-8 -*-
import os, re

os.chdir(r'/Users/jihunmin/TIL/python')

f = open('friends101.txt', 'r', encoding = 'utf-8')
script101 = f.read()

char = re.compile(r'[A-Z][a-z]+:') #패턴 객체 생성
y = set(re.findall(char, script101))
z = list(y)
character = []
for i in z:
    character += [i[:-1]]

# 리스트 컴프리헨션 사용시
# character = [x[:-1] for x in list(set(re.findall(r'[A-Z][a-z]+:', script101)))]

print(character)