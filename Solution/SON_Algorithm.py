from pyspark import SparkContext
import itertools
import sys
import csv
import math
import time

case = int(sys.argv[1])
inputfile = sys.argv[2]
support = int(sys.argv[3])

sc = SparkContext('local','prob1')

t = time.time()

infile = sc.textFile(inputfile)

rdd = infile.map( lambda x: x.split(',') )\
            .filter( lambda x: 'userId' not in x )\
            .map( lambda x: ( int(x[case-1]) , [int(x[case%2])] ) )\
            .reduceByKey( lambda x,y: x+y if y!=x else x)\
            .map( lambda x:  frozenset(list(set(x[1])))  )

# print rdd.collect()

totalbaskets = len(rdd.collect())
# rdd=rdd.repartition(2*numpart)
numpart = rdd.getNumPartitions()

minimumsupport = int(math.floor(float(support)/numpart))
   

# print "minsupp --------------->"
# print minimumsupport
    
def filterSet(data, Ck, minsupport):
    count = {}
    for d in data:
        for c in Ck:
            if c.issubset(d):
                if not c in count:
                    count[c]=1
                else: 
                    count[c] += 1
    list1 = []
    for each in count:
        supp = count[each]
        if supp >= minsupport:
            list1.insert(0,each)
    return list1

def makeKsets(Lk, k):
    list1 = []

    for i in range(len(Lk)):
        for j in range(i+1, len(Lk)): 
            L1 = list(Lk[i])[:k-2]
            L2 = list(Lk[j])[:k-2]
            L1.sort()
            L2.sort()
            if L1==L2:
                list1.append(Lk[i] | Lk[j])
                
    return list1

def apriori(partition):
    listpartition=[]
    for it in partition:
        listpartition += [it] 

    C1 = []
    for each in listpartition:
        for item in each:
            if not [item] in C1:
                C1.append([item])        
    C1.sort()
    C1= list(map(frozenset,C1))

    data = list(map(set, listpartition))
    
    L1 = filterSet(data, C1, minimumsupport)
   
    L = [L1]
    k = 2
    while (len(L[k-2]) > 0):
        Ck = makeKsets(L[k-2], k)
        Lk = filterSet(data, Ck, minimumsupport)
        L.append(Lk)
        k += 1
    
    yield L

finalrdd = rdd.mapPartitions(apriori)

freqlist = []

for lists in finalrdd.collect():
    for listitem in lists:
        freqlist+=listitem

rdd2 = sc.parallelize(freqlist)\
        .map(lambda x: (x,1))\
        .reduceByKey(lambda x,y: x)

mergedcandidates = rdd2.collect()
candidates = sc.broadcast(mergedcandidates)

list1 = rdd.map(lambda x:  [ (y[0],1) for y in candidates.value if x.issuperset(y[0]) ]   )\
                  .reduce(lambda x, y: x + y )

list2 = sc.parallelize(list1)\
            .map(lambda x: x )\
            .reduceByKey(lambda x,y: x+y )\
            .filter(lambda (x,y) : y>=support)\
            .map(lambda (x,y): sorted(x) )\

# print list2.collect()

lendict = {}

for each in list2.collect():
    if len(each) not in lendict:
        lendict[len(each)] =  [ tuple(each) ]
    else:
        lendict[len(each)] +=  [ tuple(each) ]
        
for each in lendict:
    lendict[each] = sorted(lendict[each])

# print lendict

outputfile = open("Snehal_Shirgure_SON_MovieLens.Big.case1-30000.txt","w+")

for key in lendict:
    if(key==1):
        ll = lendict[key]
        outputfile.write("("+str(ll[0][0])+")")
        for i in range(1,len(ll)):
            outputfile.write(",")
            outputfile.write("("+str(ll[i][0])+")")
        outputfile.write("\n")
    else:
        ll = lendict[key]
        outputfile.write(str(ll[0]))
        for i in range(1,len(ll)):
            outputfile.write(",")
            outputfile.write(str(ll[i]))
        outputfile.write("\n")
    outputfile.write("\n")

print(time.time()-t)

outputfile.close()



