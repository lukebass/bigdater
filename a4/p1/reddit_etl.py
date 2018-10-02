from pyspark import SparkConf, SparkContext
import sys, json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def main(inputs, output):
    text = sc.textFile(inputs)
    comments = text.map(getSubreddits).filter(lambda x: 'e' in x[0]).cache()
    comments.filter(lambda x: x[1] > 0).map(json.dumps).saveAsTextFile(output + '/positive')
    comments.filter(lambda x: x[1] <= 0).map(json.dumps).saveAsTextFile(output + '/negative')

def getSubreddits(line):
    comment = json.loads(line)
    return (comment['subreddit'], comment['score'], comment['author'])

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)