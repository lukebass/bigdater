from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import sys, re, os, gzip, datetime, uuid
assert sys.version_info >= (3, 5)

wordsep = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def main(inputs, keyspace, table):
    cluster = Cluster(['199.60.17.188', '199.60.17.216'])
    session = cluster.connect(keyspace)
    batch = BatchStatement()
    insertLog = session.prepare("INSERT INTO " + table + " (id, host, datetime, path, bytes) VALUES (?, ?, ?, ?, ?)")

    count = 0
    for f in os.listdir(inputs):
        with gzip.open(os.path.join(inputs, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                fields = getFields(line)
                
                if fields:
                    batch.add(insertLog, (fields[0], fields[1], fields[2], fields[3], fields[4]))
                    count += 1
                
                if count > 300:
                    session.execute(batch)
                    batch.clear()
                    count = 0

    if count > 0:
        session.execute(batch)

def getFields(line):
    fields = wordsep.split(line)
    if (len(fields) > 3):
        return (uuid.uuid4(), fields[1], datetime.datetime.strptime(fields[2], '%d/%b/%Y:%H:%M:%S'), fields[3], int(fields[4]))

if __name__ == '__main__':
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    main(inputs, keyspace, table)