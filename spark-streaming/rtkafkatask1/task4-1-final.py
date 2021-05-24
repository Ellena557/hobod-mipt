import os
import time
import subprocess
import hyperloglog

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from hdfs import Config
from ua_parser import user_agent_parser


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

DATA_PATH = "/data/course4/uid_ua_100k_splitted_by_5k"
batches = [sc.textFile(os.path.join(*[nn_address, DATA_PATH, path])) for path in client.list(DATA_PATH)]  
BATCH_TIMEOUT = 2
ssc = StreamingContext(sc, BATCH_TIMEOUT)


success = False

# use final_res to write the result exactly once
final_res = []

def print_rdd(rdd):
    global success
    global final_res
    if success:
        final_res = rdd.collect()

def set_success_flag(rdd):
    global success
    if rdd.isEmpty():
        success = True

dstream = ssc.queueStream(rdds=batches)
dstream.foreachRDD(set_success_flag)

def get_seg_type(text):
    user_id, addr = text.strip().split("\t")
    res = []
    if 'iPhone' in user_agent_parser.Parse(addr)['device']['family']:
        res.append("seg_iphone")
    if 'Firefox' in user_agent_parser.Parse(addr)['user_agent']['family']:
        res.append("seg_firefox")
    if 'Windows' in user_agent_parser.Parse(addr)['os']['family']:
        res.append("seg_windows")

    formatted = []
    for r in res:
        formatted.append((r, user_id))
    return formatted

def aggregator(values, old):
    if old is None:
        old = hyperloglog.HyperLogLog(0.01)
    for v in values:
        old.add(v)
    return old
	

ssc.checkpoint('./checkpoint{}'.format(time.strftime("%Y_%m_%d_%H_%M_%s", time.gmtime()))) 

result = dstream \
    .flatMap(get_seg_type) \
    .updateStateByKey(aggregator) \
    .map(lambda type: (type[0], len(type[1]))) \
    .foreachRDD(lambda rdd: print_rdd(rdd.sortBy(lambda x: -x[1])))

ssc.start()

while not success:
    time.sleep(0.05)
	
ssc.stop()
sc.stop()

for elem in final_res:
    print('{}\t{}'.format(*elem))
