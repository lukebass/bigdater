from pyspark import SparkConf, SparkContext
import sys, re

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

wordsep = re.compile(r'[\s]+')

def getPageValues(line):
    values = wordsep.split(line)
    return (values[0], values[1], values[2], int(values[3]), values[4])

def filterPages(page):
    if page[1] == "en" and page[2] != "Main Page" and not page[2].startswith("Special:"):
        return True

def getDateViewsPairs(page):
    return (page[0], (page[3], page[2]))

def getKey(kv):
    return kv[0]

def outputFormat(kv):
    k, v = kv
    return '%s (%i, %s)' % (k, v[0], v[1])

text = sc.textFile(inputs)
pages = text.map(getPageValues).filter(filterPages).map(getDateViewsPairs)
maxViews = pages.reduceByKey(max)

outdata = maxViews.sortBy(getKey).map(outputFormat)
outdata.saveAsTextFile(output)
