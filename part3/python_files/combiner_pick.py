# -*- coding: utf-8 -*-

from operator import itemgetter
import sys
import os

lst = [[(0,0)]*6]*13 

def func (a,b) :
    words = a.split(',')
    index =int(words[0])
    global lst
    lst[index][5] = (a,b)
    lst[index] = sorted(lst[index],key= lambda c : c[1],reverse=True)


for file in os.listdir("outputpart_31_1_05"):
    if not(file.startswith("part")) :
        continue
    # print file
    file_object  = open("outputpart_31_1_05/"+file, 'r')
    for line in file_object:
        line = line.replace('\',',':')
        line = line.replace('\'','')
        line = line.replace('(','')
        line = line.replace(')','')
        line = line.strip()

        # print line
        word ,count= line.split(':')
        count = int(count)
        # print word,count
        # count = int(count)
        
        func(word, count)
          

for j in range(1,13):
    new_file = open ("final_monthwise_outputs_pick/output_month_"+str(j),"w")
    for i in range(0,5):
        new_file.write(str(lst[j][i])+"\n")
    new_file.close()