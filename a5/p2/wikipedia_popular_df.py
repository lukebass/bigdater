from pyspark.sql import SparkSession, functions, types
import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
spark = SparkSession.builder.appName('reddit average').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

@functions.udf(returnType=types.StringType())
def pathToHour(path):
    return os.path.splitext(os.path.basename(path))[0]

def main(inputs, output):
    wiki_schema = types.StructType([
        types.StructField('language', types.StringType(), True),
        types.StructField('title', types.StringType(), True),
        types.StructField('views', types.LongType(), True),
        types.StructField('size', types.LongType(), True)
    ])

    views = spark.read.csv(inputs, schema=wiki_schema, sep=' ').withColumn('hour', pathToHour(functions.input_file_name()))
    views = views.filter(views.language == 'en').filter(views.title != 'Main Page').filter(~views.title.startswith("Special:"))
    
    maxViews = views.groupBy('hour').max('views')
    maxPages = views.join(maxViews, [views.hour == maxViews.hour, views.views == maxViews['max(views)']]).drop(maxViews.hour).orderBy('hour', 'title')
    maxPages[['hour', 'title', 'views']].write.json(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)