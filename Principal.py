import sys
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 20)

    kvs = KafkaUtils.createDirectStream(ssc, ["test7"], {"metadata.broker.list": "localhost:9092"})

    es_write_conf = {
        "es.nodes": "localhost",
        "es.port": "9200",
        "es.resource": "testindex/testdoc",
        "es.input.json": "yes",
        "es.mapping.id": "doc_id"
    }

    def format_data(x):
        return (x["doc_id"], json.dumps(x))

    def send_data(rdd):
        if rdd is not None:
            data = rdd.collect()
            data = map(lambda item: json.loads(item),  data)

            formatted_rdd = sc.parallelize(data)

            formatted_rdd= formatted_rdd.map(lambda x: format_data(x))

            formatted_rdd.saveAsHadoopFile(
                path='-',
                outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                keyClass="org.apache.hadoop.io.NullWritable",
                valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                conf=es_write_conf)

    dStream = kvs.map(lambda x: x[1])
    dStream.foreachRDD(lambda rdd: send_data(rdd))

    ssc.start()
    ssc.awaitTermination()
