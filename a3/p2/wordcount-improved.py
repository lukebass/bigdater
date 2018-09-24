from pyspark import SparkConf, SparkContext
import sys, operator, re, string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

def main(inputs, output):
    text = sc.textFile(inputs)
    words = text.flatMap(words_once).filter(lambda x: len(x[0]) > 0)
    wordcount = words.reduceByKey(operator.add)

    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)

def words_once(line):
    for w in wordsep.split(line):
        yield (w.lower(), 1)

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
