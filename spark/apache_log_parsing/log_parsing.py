from pyspark.sql import Row
import re

PATTERN = '^(\S+)\.(\S+)\.(\S+) \- \- \[([a-zA-Z0-9]+/[a-zA-Z0-9]+/[a-zA-Z0-9]+\:[0-9]+\:[0-9]+\:[0-9]+ \-[0-9]+)] "([A-Za-z]+ \/[a-zA-Z]+(.*))" ([0-9]+) ([0-9]+)'

#Example line
line = 'slppp6.intermind.net - - [01/Aug/1995:00:00:39 -0400] "GET /history/skylab/skylab-logo.gif HTTP/1.0" 200 3274'
#line = 'piweba4y.prodigy.com - - [01/Aug/1995:00:00:10 -0400] "GET /images/launchmedium.gif HTTP/1.0" 200 11853'
#line = 'uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] "GET / HTTP/1.0" 304 0'

def parseLogLine(log):
    m = re.match(PATTERN, log)
    if m:
        return [Row(host=m.group(1), timeStamp=m.group(4),url=m.group(6), httpCode=int(m.group(7)))]
    else:
        return []

#Test if it is working
parseLogLine(line)
#output will be like [Row(host='slppp6', timestamp='01/Aug/1995:00:00:39 -0400', url='/skylab/skylab-logo.gif HTTP/1.0', httpCode='200')]

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PythonWordCountlogparsing").getOrCreate()
sc = spark.sparkContext

logFile = sc.textFile("/data/spark/project/NASA_access_log_Aug95.gz")

accessLog = logFile.flatMap(parseLogLine)
accessDf = spark.createDataFrame(accessLog)
accessDf.printSchema()
accessDf.createOrReplaceTempView("nasalog")
#output = spark.sql("select * from nasalog")
#output.createOrReplaceTempView("nasa_log")

# Uncomment this for optimizations only
#spark.sql("cache TABLE nasa_log")

spark.sql("select url,count(*) as req_cnt from nasalog where upper(url) like '%HTML%' group by url order by req_cnt desc LIMIT 10").show()

spark.sql("select host,count(*) as req_cnt from nasalog group by host order by req_cnt desc LIMIT 5").show()

spark.sql("select substr(timeStamp,1,14) as timeFrame,count(*) as req_cnt from nasalog group by substr(timeStamp,1,14) order by req_cnt desc LIMIT 5").show()

spark.sql("select substr(timeStamp,1,14) as timeFrame,count(*) as req_cnt from nasalog group by substr(timeStamp,1,14) order by req_cnt  LIMIT 5").show()

spark.sql("select httpCode,count(*) as req_cnt from nasalog group by httpCode ").show()
