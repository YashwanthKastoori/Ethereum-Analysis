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
        int(fields[5])
        return True
    except:
        pass


# Load the dataset, filter the values
transacs = sc.textFile("/data/ethereum/transactions")
good_transacs = transacs.filter(check_transaction)
transactions_split = good_transacs.map(lambda x: x.split(','))
time_epoch_price = transactions_split.map(lambda j: (int(j[6]), int(j[5]), 1))

# Map the values, taking the time_epoch and changing into month and year
yearly_price = time_epoch_price.map(lambda t: (time.strftime("%m%Y", time.gmtime(t[0])), (t[1], t[2])))

# Reduce by month and year, adding the aggregate price and count.Find the average using aggregate
yearly_agg_price = yearly_price.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
yearly_average_price = yearly_agg_price.map(lambda x: (x[0], (x[1][0] / x[1][1])))

yearly_average_price.saveAsTextFile('part_c_gas')


