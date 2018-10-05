from pyspark import SparkConf, SparkContext
import sys, json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def main(inputs, output):
    text = sc.textFile(inputs).map(json.loads).cache()
    averages = text.map(lambda x: (x['subreddit'], (1, x['score']))).reduceByKey(combineComments).map(getAverage).filter(lambda x: x[1] > 0)
    comments = text.map(lambda x: (x['subreddit'], x)).join(averages).map(getRelativeScore)
    comments.sortBy(lambda x: x[0], False).map(json.dumps).saveAsTextFile(output)

def combineComments(accum, comment):
    return (accum[0] + comment[0], accum[1] + comment[1])

def getAverage(kv):
    k, v = kv
    return (k, v[1]/v[0])

def getRelativeScore(kv):
    v = kv[1]
    return (v[0]['score']/v[1], v[0]['author'])

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)