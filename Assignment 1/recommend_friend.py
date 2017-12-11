#####################################################################
# Name: Waghe Shubham Yatindra
# Roll No.: 13MF3IM17
# Apache Spark M/R python program to find friend recommendations
# Command: spark-submit <python file> <data file>
#####################################################################
from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from itertools import combinations

def map_screen_func(x):
    a = x.value.split("\t")
    return (a[0],a[1].split(','))

def emit_connections(s):
    key, friends = s[0], s[1]
    l = []
    if friends != '' and len(friends) != 0:
        for friend in friends:
            if friend != '':
                l.append((int(key), { int(friend): -1 }))
        c = combinations(friends,2)
        for f in c:
            p = list(f)
            l.append((int(p[0]), { int(p[1]): 1 }))
            l.append((int(p[1]), { int(p[0]): 1 }))
    return l

def my_reducer(p,q):
    for key in q:
        if q[key] == -1:
            p[key] = q[key]
            continue
        if key in p:
            if p[key] != -1:
                p[key] = int(p[key]) + int(q[key])
        else:
            p[key] = q[key]
    return p

def cleaner_func(x):
    values = dict(x[1])
    values = { key:val for key, val in values.items() if val != -1 }
    values = sorted(values.items(), key=lambda x: (-x[1], x[0]))
    l,i = [], 0
    for k in values:
        if i == 10:
            break
        l.append(k)
        i = i + 1
    return int(x[0]), l

def writer_func(t):
    w_string = str(t[0])
    fs = [ a[0] for a in t[1]]
    w_string += '\t'+','.join(map(str, fs))
    return w_string

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Incorrect Usage!")
        exit(-1)

    spark = SparkSession.builder.appName("RecommendFriend").getOrCreate()
    lines = spark.read.text(sys.argv[1]).rdd.map(map_screen_func)
    connections = lines.flatMap(emit_connections)
    formatter = connections.reduceByKey(lambda k,v : my_reducer(k,v)).map(cleaner_func).sortByKey()
    # formatter = connections.reduceByKey(lambda k,v : my_reducer(k,v)).map(cleaner_func).sortByKey()
    w = formatter.map(lambda t : writer_func(t))
    w.saveAsTextFile('output')
    spark.stop()
