from __future__ import division
from __future__ import print_function
import os
import re
import random
import time
import binascii
from bisect import bisect_right
from heapq import heappop, heappush
import pandas as pd
import numpy as np
df = pd.read_csv('Data.txt',sep='\t',header=None)
cols = [2,3,4]
df.drop(df.columns[cols],axis=1,inplace=True)
df = df.dropna(axis=0, how='all')
df.columns = df.iloc[0]
df = df.iloc[1:]
df['Para'] = df['Para'].apply(lambda x: str(x).replace('\n',' '))
numDocs = 500


curShingleID = 0
docsAsShingleSets = {};
docNames = []
totalShingles = 0
for index,row in df.iterrows():
    words = row['Para'] 
    docNames.append(row['ParaNo']) 
    docID = row['ParaNo']
    shinglesInDoc = set()
    for i in range(0, len(words) - 4):
        shingle = words[i]+words[i + 1]+words[i + 2]+words[i+3]+words[i+4]
        crc = binascii.crc32(shingle) & 0xffffffff
        shinglesInDoc.add(crc)
    docsAsShingleSets[docID] = shinglesInDoc
    totalShingles = totalShingles + (len(words) - 5)


maxShingleID = 2**32-1
nextPrime = 4294967311
numHashes = 10
def pickRandomCoeffs(k):
    randList = []
    while k > 0:
        randIndex = random.randint(0, maxShingleID) 
        while randIndex in randList:
            randIndex = random.randint(0, maxShingleID) 
        randList.append(randIndex)
        k = k - 1
    return randList
coeffA = pickRandomCoeffs(numHashes)
coeffB = pickRandomCoeffs(numHashes)
signatures = []
for docID in docNames:
    shingleIDSet = docsAsShingleSets[docID] 
    signature = []
    for i in range(0, numHashes):
        minHashCode = nextPrime + 1
        for shingleID in shingleIDSet:
            hashCode = (coeffA[i] * shingleID + coeffB[i]) % nextPrime 
            if hashCode < minHashCode:
                minHashCode = hashCode
        signature.append(minHashCode)
    signatures.append(signature)

def getTriangleIndex(i, j):
    if i == j:
        sys.stderr.write("Can't access triangle matrix with i == j")
        sys.exit(1)
    if j < i:
        temp = i
        i = j
        j = temp
    k = int(i * (numDocs - (i + 1) / 2.0) + j - i) - 1
    return k
numElems = int(numDocs * (numDocs - 1) / 2)
estJSim = [0 for x in range(numElems)]
for i in range(0, numDocs):
    signature1 = signatures[i]
    for j in range(i+1, numDocs):
        signature2 = signatures[j]
        count = 0
        for k in range(0, numHashes):
            count = count + (signature1[k] == signature2[k])   
        estJSim[getTriangleIndex(i, j)] = (count / numHashes)


JSim = [0 for x in range(numElems)]
if numDocs <= 2500:
    for i in range(0, numDocs):
        s1 = docsAsShingleSets[docNames[i]]
        for j in range(i + 1, numDocs):
            s2 = docsAsShingleSets[docNames[j]]
            JSim[getTriangleIndex(i, j)] = (len(s1.intersection(s2)) / len(s1.union(s2)))    


mat = np.zeros(shape=(numDocs,numDocs))
for i in range(0,numDocs):
    for j in range(0,numDocs):
        if i==j:
            mat[i][j]=1.0
        else:
            mat[i][j] = estJSim[getTriangleIndex(i, j)]
mat1 = np.zeros(shape=(numDocs,numDocs))
for i in range(0,numDocs):
    for j in range(0,numDocs):
        if i==j:
            mat1[i][j]=1.0
        else:
            mat1[i][j] = JSim[getTriangleIndex(i, j)]
import sklearn
from sklearn.cluster import KMeans
eigen_values, eigen_vectors = np.linalg.eigh(mat)
k1 = KMeans(n_clusters=2, init='k-means++').fit_predict(eigen_vectors)
eigen_values, eigen_vectors = np.linalg.eigh(mat1)
k2 = KMeans(n_clusters=2, init='k-means++').fit_predict(eigen_vectors)
book1 = list(np.where(k1==0)[0])
book2 = list(np.where(k1==1)[0])
s1 = "Book 1: " + (",").join(str(x+1) for x in book1)
s2 = "Book 2: " + (",").join(str(x+1) for x in book2)
with open("Output.txt", "w") as text_file:
    text_file.write(s1+'\n'+s2)

dists = sklearn.metrics.pairwise_distances(mat1[:,book1])
dists[np.tril_indices_from(dists, -1)] = 0
b1_5 = np.unravel_index(np.argsort(dists.ravel())[-5:], dists.shape)
dists = sklearn.metrics.pairwise_distances(mat1[:,book2])
dists[np.tril_indices_from(dists, -1)] = 0
b2_5 = np.unravel_index(np.argsort(dists.ravel())[-5:], dists.shape)


with open("Output1.txt", "a") as text_file:
    text_file.write("Book 1: " + '\n')
for i in range(0,5):
    words1 = df['Para'][b1_5[0][i]]
    words2 = df['Para'][b1_5[1][i]]
    shinglesInDoc1 = set()
    shinglesInDoc2 = set()
    for j in range(0, len(words1) - 4):
        shingle = words1[j]+words1[j + 1]+words1[j + 2]+words1[j+3]+words1[j+4]
        shinglesInDoc1.add(shingle)
    for j in range(0, len(words2) - 4):
        shingle = words2[j]+words2[j + 1]+words2[j + 2]+words2[j+3]+words2[j+4]
        shinglesInDoc2.add(shingle)
    indices1 = [k for (k, x) in enumerate(shinglesInDoc1) if x in shinglesInDoc1.intersection(shinglesInDoc2)]
    indices2 = [k for (k, x) in enumerate(shinglesInDoc2) if x in shinglesInDoc1.intersection(shinglesInDoc2)]
    for l in range(0,len(indices1)):
        with open("Output1.txt", "a") as text_file:
            s1 = "<" + str(b1_5[0][i]) + "--" + str(b1_5[1][i]) + ">" +" "
            s2 = "<" + list(shinglesInDoc1)[indices1[l]] + ">"+" "
            s3 = "<" + str(indices1[l]) + "--" + str(indices2[l]) +">"+" "
            text_file.write(s1+s2+s3+'\n')
with open("Output1.txt", "a") as text_file:
    text_file.write("Book 2: " + '\n')
for i in range(0,5):
    words1 = df['Para'][b2_5[0][i]]
    words2 = df['Para'][b2_5[1][i]]
    shinglesInDoc1 = set()
    shinglesInDoc2 = set()
    for j in range(0, len(words1) - 4):
        shingle = words1[j]+words1[j + 1]+words1[j + 2]+words1[j+3]+words1[j+4]
        shinglesInDoc1.add(shingle)
    for j in range(0, len(words2) - 4):
        shingle = words2[j]+words2[j + 1]+words2[j + 2]+words2[j+3]+words2[j+4]
        shinglesInDoc2.add(shingle)
    indices1 = [k for (k, x) in enumerate(shinglesInDoc1) if x in shinglesInDoc1.intersection(shinglesInDoc2)]
    indices2 = [k for (k, x) in enumerate(shinglesInDoc2) if x in shinglesInDoc1.intersection(shinglesInDoc2)]
    for l in range(0,len(indices1)):
        with open("Output1.txt", "a") as text_file:
            s1 = "<" + str(b2_5[0][i]) + "--" + str(b2_5[1][i]) + ">" +" "
            s2 = "<" + list(shinglesInDoc1)[indices1[l]] + ">"+" "
            s3 = "<" + str(indices1[l]) + "--" + str(indices2[l]) +">"+" "
            text_file.write(s1+s2+s3+'\n')
