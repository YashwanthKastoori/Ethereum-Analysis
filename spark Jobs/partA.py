import pyspark
import time
import re  #

WORD_REGEX = re.compile(r"\b\w+\b")

# This function checks good lines of transactions

sc = pyspark.SparkContext()


def check_transaction(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        int(fields[6])
        return True
    except:
        pass


transacs = sc.textFile("/data/ethereum/transactions")
good_transacs = transacs.filter(check_transaction)
time_epoch = good_transacs.map(lambda a: int(a.split(',')[6]))
date_month_year = time_epoch.map(lambda t: (time.strftime("%m%Y", time.gmtime(t)), 1))
res = date_month_year.reduceByKey(lambda x, y: x + y)
res.saveAsTextFile('part_a')
