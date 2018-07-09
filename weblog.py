import re
import datetime
from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/tmp").appName("Weblog").getOrCreate()

apache_access_log_pattern = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)'

month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

def parse_apache_time(s):
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


def parse_apace_log_line(logline):
    match = re.search(apache_access_log_pattern, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = 0
    else:
        size = int(match.group(9))
    return (Row(
        ipAddress = match.group(1),
        clientId = match.group(2),
        userId = match.group(3),
        dateTime = parse_apache_time(match.group(4)),
        method = match.group(5),
        endpoint = match.group(6),
        protocol = match.group(7),
        responseCode = int(match.group(8)),
        contentSize = size
    ), 1)

sc = spark.sparkContext
parsed_logs = sc.textFile("access_log_Jul95").map(parse_apace_log_line).cache()
access_logs = parsed_logs.filter(lambda x: x[1] == 1).map(lambda x: x[0]).cache()
failed_logs = parsed_logs.filter(lambda x: x[1] == 0).map(lambda x: x[0])

print('Read %d lines, successfully parsed %d lines, failed to parse %d lines'
      % (parsed_logs.count(), access_logs.count(), failed_logs.count()))

access_logs_df = spark.createDataFrame(access_logs)
access_logs_rdd = access_logs_df.rdd.repartition(1).map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8]))\
    .saveAsTextFile("E:\\projects\\twitter\\weblogDf1")

ip_count = access_logs_df.groupBy("ipAddress").count()
ip_count_rdd = ip_count.rdd.repartition(1).map(lambda x: (x[0], x[1]))\
   .saveAsTextFile("E:\\projects\\twitter\\response11")

response_count = access_logs_df.groupBy("responseCode").count()
response_count_rdd = response_count.rdd.repartition(1).map(lambda x: (x[0], x[1]))\
   .saveAsTextFile("E:\\projects\\twitter\\response")

sum_size = access_logs_df.groupBy("ipAddress").sum("contentSize")
sum_size_rdd = sum_size.rdd.repartition(1).map(lambda x: (x[0], x[1]))\
   .saveAsTextFile("E:\\projects\\twitter\\sum_size")

page_hit = access_logs_df.groupBy("endpoint").count()
page_hit_rdd = page_hit.rdd.repartition(1).map(lambda x: (x[0], x[1]))\
   .saveAsTextFile("E:\\projects\\twitter\\page_hits")

date_count = access_logs_df.groupBy("dateTime").count()
date_count_rdd = date_count.rdd.repartition(1).map(lambda x: (x[0], x[1]))\
    .saveAsTextFile("E:\\projects\\twitter\\date_count")

spark.stop()