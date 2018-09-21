from pyspark import SparkConf, SparkContext
import sys, json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def main(inputs, output):
    text = sc.textFile(inputs)
    comments = text.map(getSubreddits)
    averages = comments.reduceByKey(combineComments).map(getAverage)

    outdata = averages.sortBy(getKey).map(json.dumps)
    outdata.saveAsTextFile(output)

def getSubreddits(line):
    comment = json.loads(line)
    return (comment['subreddit'], (1, comment['score']))

def combineComments(accum, comment):
    return (accum[0] + comment[0], accum[1] + comment[1])

def getAverage(kv):
    k, v = kv
    return (k, v[1]/v[0])

def getKey(subreddit):
    return subreddit[0]

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)