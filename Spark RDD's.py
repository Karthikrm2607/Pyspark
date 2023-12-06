# Databricks notebook source
12+3

# COMMAND ----------

#import the spark RDD credentials 
from pyspark import SparkConf, SparkContext
conf=SparkConf().setAppName("Rdd")
sc=SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

#Import the file
text=sc.textFile('/FileStore/tables/sample.txt')
text.collect()

# COMMAND ----------

##### RDD FUNCTIONS ###
# """
# 1.map(lambda x : x.split(' '))
# 2.flatmap(lambda x : x.split())
# 3.filter(lambda x : x=value)
# 4.distinct(*)
# 5.groupByKey().mapValues(list)                ==> data should be (key, value)
# 6.reduceByKey(lambda x,y:x+y)               ==> data should be (key, value)
# 7.count('value')
# 8.countByValue()
# 9.saveAsTextFile('path')
# 10.repartition(number of partition)
# 11.getNumPartition()
# 12.coalesce()
# 13.aggregate functions(we can write them by logic)
# """

# COMMAND ----------

##### map ##### creates new rdd #####

# COMMAND ----------

rdd1=sc.textFile('/FileStore/tables/sample.txt')
rdd1.collect()

# COMMAND ----------

mapRDD=rdd1.map(lambda x : x.split(' '))  #1 spaces
mapRDD.collect()

# COMMAND ----------

junkrdd3=rdd1.map(lambda x : x+' karthik')
junkrdd3.collect()

# COMMAND ----------

junkrdd4=rdd1.map(lambda x : x+' karthik').map(lambda x : x.split(' '))
junkrdd4.collect()

# COMMAND ----------

hi_fileRDD=sc.textFile('/FileStore/tables/hi_sample.txt')
hi_fileRDD.collect()

# COMMAND ----------

hi_fileRDD2=hi_fileRDD.map(lambda x : x.split(' '))
hi_fileRDD2.collect()

# COMMAND ----------

hi_fileRDD3=hi_fileRDD2.map(lambda x : [len(i) for i in x])
hi_fileRDD3.collect()

# COMMAND ----------

#### flatmap #### all rows gets into one row #######
####map = only elements of that row gets split to that row itself ####

# COMMAND ----------

mapRDD.collect()

# COMMAND ----------

flatmapRDD=rdd1.flatMap(lambda x : x.split(' '))
flatmapRDD.collect()

# COMMAND ----------

#### filter #### shows only that element ####

# COMMAND ----------

filterRDD=mapRDD.filter(lambda x : x==2)
filterRDD.collect()

# COMMAND ----------

filterRDD=mapRDD.filter(lambda x : x==['1', '2', '3', '4', '5'])
filterRDD.collect()

# COMMAND ----------

filterRDD2 = flatmapRDD.filter(lambda x: x == '2')
filterRDD2.collect()

# COMMAND ----------

filterRDD2 = flatmapRDD.filter(lambda x: int(x) == 2)
filterRDD2.collect()

# COMMAND ----------

this_mangoRDD=sc.textFile("/FileStore/tables/this_mango.txt")
this_mangoRDD.collect()

# COMMAND ----------

this_mangoFlatMap=this_mangoRDD.flatMap(lambda x : x.split(' '))
this_mangoFlatMap.collect()

# COMMAND ----------

this_mangoFilter=this_mangoFlatMap.filter(lambda x : not (x.startswith('c') or x.startswith('a')))
this_mangoFilter.collect()

# COMMAND ----------



# COMMAND ----------

flatmapRDD.collect()

# COMMAND ----------

#### distinct ####

# COMMAND ----------

distinctRDD=flatmapRDD.distinct()
distinctRDD.collect()

# COMMAND ----------

#### groupByKey #### count of g in ==>'g', 'g' ==>(count 1, count 1)

# COMMAND ----------

this_mangoRDD2=sc.textFile("/FileStore/tables/this_mango.txt")
this_mangoRDD2.collect()

# COMMAND ----------

this_mangoRDD2flatmap=this_mangoRDD2.flatMap(lambda x : x.split(' ')).map(lambda x : (x,1))
this_mangoRDD2flatmap.collect()

# COMMAND ----------

this_mangoRDD2groupby=this_mangoRDD2flatmap.groupByKey().mapValues(list)
this_mangoRDD2groupby.collect()

# COMMAND ----------

#### reduce by key #### count of g in ==> 'g', 'g' ==> (count 2)

# COMMAND ----------

this_mangoRDD2reducebyby=this_mangoRDD2flatmap.reduceByKey(lambda x,y:x+y)
this_mangoRDD2reducebyby.collect()

# COMMAND ----------

thatmangoRDD=sc.textFile('/FileStore/tables/that_mango.txt')
thatmangoRDD.collect()

# COMMAND ----------

thatmangoRDDgroupby=thatmangoRDD.flatMap(lambda x : x.split(' ')).map(lambda x : (x,1)).groupByKey().mapValues(list)
thatmangoRDDgroupby.collect()

# COMMAND ----------

thatmangoRDDreduceby=thatmangoRDD.flatMap(lambda x : x.split(' ')).map(lambda x : (x,1)).reduceByKey(lambda x,y:x+y)
thatmangoRDDreduceby.collect()

# COMMAND ----------

#### count ######
name = 'karthik'
name.count('k')

# COMMAND ----------

#### countByValue ####
thatmangoRDDreduceby.countByValue()

# COMMAND ----------

that_mangoRDD_countby_value=thatmangoRDD.flatMap(lambda x : x.split(' '))
that_mangoRDD_countby_value.countByValue()

# COMMAND ----------

#### saveAsTextFile('path') ####
that_mangoRDD_countby_value.saveAsTextFile('/FileStore/tables/that_mangoRDD_countby_value1.txt')

# COMMAND ----------

#### getnumPartitions() ####
that_mangoRDD_countby_value.getNumPartitions()

# COMMAND ----------

#### repartition ####
that_mangoRDD_countby_value1=that_mangoRDD_countby_value.repartition(5)
that_mangoRDD_countby_value1.getNumPartitions()

# COMMAND ----------

#### coalesce ####
that_mangoRDD_countby_value1=that_mangoRDD_countby_value1.coalesce(3)
that_mangoRDD_countby_value1.getNumPartitions()

# COMMAND ----------

#### Project-1 on RDD ####
#1 . With studentDaata.csv do below operations
# a) show total_students, total_marks_of_males, 
#     total_marks_of_females, 
#     total_students_passed(50+marks)
# b) students per course
# c) total_marks per course
# d) Show min and max marks per course
# e)avg age of male/female





studentdata_rdd=sc.textFile("/FileStore/tables/StudentData.csv")
studentdata_rdd.collect()

# COMMAND ----------

header=studentdata_rdd.first()
studentdata_rdd1=studentdata_rdd.filter(lambda x: x!=header)

# COMMAND ----------

studentdata_rdd1.count()    #gives total students

# COMMAND ----------

total_marks = studentdata_rdd1 \
    .map(lambda x: x.split(',')) \
    .map(lambda x: (x[1], int(x[5]))) \
    .reduceByKey(lambda x, y: x + y)
total_marks.collect()

# total_marks=studentdata_rdd1 \
#     .map(lambda x : x.split(',')) \     #total marks of females and males
#     .map(lambda x : (x[1],int(x[5]))) \
#     .reduceByKey(lambda x,y = x+y) \


# COMMAND ----------

#failed students
failed_students = studentdata_rdd1 \
    .map(lambda x: x.split(',')) \
    .map(lambda x: int(x[5])) \
    .filter(lambda x : int(x<50)) \
#    .filter(lambda x : (x<50)) \
failed_students.count()

# COMMAND ----------

#passed students
passed_students = studentdata_rdd1 \
    .map(lambda x: x.split(',')) \
    .map(lambda x: int(x[5])) \
    .filter(lambda x : x>50 or x==50) \
#    .filter(lambda x : (x<50)) \
passed_students.count()

# COMMAND ----------

students_course=studentdata_rdd1 \
    .map(lambda x: x.split(',')) \
    .map(lambda x : (x[3]))
    
students_course.countByValue()      #number of students per course

# COMMAND ----------

#or
students_course=studentdata_rdd1 \
    .map(lambda x: x.split(',')) \
    .map(lambda x : (x[3]))\
    .map(lambda x : (x,1)) \
    .reduceByKey(lambda x,y:x+y)
    
students_course.collect()      #number of students per course

# COMMAND ----------

#total marks per course
course_marks=studentdata_rdd1 \
    .map(lambda x: x.split(',')) \
    .map(lambda x : (x[3],int(x[5])))\
    .reduceByKey(lambda x,y:x+y)
    
course_marks.collect()

# COMMAND ----------


