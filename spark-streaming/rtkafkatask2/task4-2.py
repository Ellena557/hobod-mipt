import os
import time
import subprocess

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from hdfs import Config
from pyspark.sql import SparkSession


conf_file = "hdfscli.cfg"
str = """
[global]
default.alias = default
[default.alias]
url = http://mipt-master.atp-fivt.org:50070
user = hob2021160
"""

with open(conf_file, "w") as f:
    f.write(str)
client = Config(conf_file).get_client()

nn_address = subprocess.check_output('hdfs getconf -confKey dfs.namenode.http-address', shell=True).strip().decode("utf-8")
sc = SparkContext(master='yarn-client') 

# initialize to read parquet file 
spark_par = SparkSession.builder \
    .master('yarn') \
    .appName('friends') \
    .getOrCreate()

DATA_PATH = "/data/graphDFQuarter"
batches = [spark_par.read.parquet(os.path.join(*[nn_address, DATA_PATH, path])).rdd for path in client.list(DATA_PATH)]
BATCH_TIMEOUT = 10
ssc = StreamingContext(sc, BATCH_TIMEOUT)


success = False

# use final_res to write the result
final_res = []

def set_success_flag(rdd):
    global success
    if rdd.isEmpty():
        success = True

dstream = ssc.queueStream(rdds=batches)
dstream.foreachRDD(set_success_flag)

def find_most_common(new_values):
    global final_res
    # add new values
    new_res = final_res + new_values
    #sort again
    sorted_res = sorted(new_res, reverse = True)
    # return only top 50
    final_res = sorted_res[:50]

def get_common(text):
    (user_id_1, friends_1), (user_id_2, friends_2) = text
    commons = list(set(friends_1) & set(friends_2))
    return len(commons), user_id_1, user_id_2

result = dstream \
    .transform(lambda rdd: rdd.cartesian(rdd)) \
    .map(get_common) \
    .filter(lambda x: x[1] < x[2]) \
    .foreachRDD(lambda rdd: find_most_common(rdd.sortBy(lambda x: -x[0]).take(50)))

ssc.start()

while not success:
    time.sleep(0.05)

ssc.stop()
sc.stop()

for elem in final_res:
    print('{}\t{}\t{}'.format(*elem))
