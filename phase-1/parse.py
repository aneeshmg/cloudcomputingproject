import re

with open('./sample.txt') as f:
    res = f.read()

# print(res)

s = res.split('Objects:')

for i,x in zip(s,range(len(s))):
    i = i.strip()
    print(x,i)