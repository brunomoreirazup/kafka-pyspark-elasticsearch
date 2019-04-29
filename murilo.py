from __future__ import print_function

import sys
import re

from kafka import KafkaProducer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col
from pyspark.streaming.kafka import KafkaUtils



def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: script_name.py <broker_list> <topic_read> <topic_write>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafka")
    ssc = StreamingContext(sc, 3)

    brokers = sys.argv[1]
    topic_read = sys.argv[2]
    topic_write = sys.argv[3]

    sc.setLogLevel("OFF")

    def send_to_kafka(rows):
        producer = KafkaProducer(bootstrap_servers = brokers)
        for row in rows:
            producer.send(topic_write,str(row.asDict()))
            producer.flush()

    def process(time, rdd):
        print("========= %s =========" % str(time))

        try:
            spark = getSparkSessionInstance(rdd.context.getConf())
            rows = rdd.map(lambda p: Row(Timestamp=(p[0] + "T" + p[1]),ID=int(p[3]),Event=p[5]))
            schemaLogs = spark.createDataFrame(data=rows)
            schemaLogs.createOrReplaceTempView("logs")
            df=spark.sql('SELECT saving.ID,saving.Timestamp as TimeStart,saved.Timestamp as TimeEnd FROM logs AS saving INNER JOIN logs AS saved ON saving.ID = saved.ID AND saving.Event="saving" WHERE saved.Event="saved"')\
                .withColumn('TimeStart', col('TimeStart').cast('timestamp'))\
                .withColumn('TimeEnd', col('TimeEnd').cast('timestamp'))\
                .withColumn('Duration (ms)', ((col('TimeEnd').cast('double') - (col('TimeStart').cast('double'))) * 1000).cast('int'))
            # df.show(10, False)
            df.select(df['ID'],df['Duration (ms)']).foreachPartition(send_to_kafka)

        except:
            pass
    kvs = KafkaUtils.createDirectStream(ssc, [topic_read], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    words = lines.map(lambda line: re.split('\s|=',line))
    words.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()