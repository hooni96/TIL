# -*- coding: utf-8 -*-
import os, re

os.chdir(r'/Users/jihunmin/TIL/python')

# 특정 단어의 예문만 모아 파일로 저장하기
f = open('friends101.txt', 'r')

# f.read(100)
# f.seek(0)

sentences = f.readlines()
lines = [i for i in sentences if re.match(r'[A-Z][a-z]+:', i)] # 대사만 따오기
would = [i for i in sentences if re.match(r'[A-Z][a-z]+:', i) and re.search('would', i)]

# would 들어 간 문장 한 줄씩 출력
# for i in would:
#    print(i)

newf = open('would.txt', 'w')
newf.writelines(would)
newf.close()