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
        int(fields[3])
        return True
    except:
        pass

transacs = sc.textFile("/data/ethereum/transactions")
good_transacs = transacs.filter(check_transaction)
to_address = good_transacs.map(lambda a: (a.split(',')[2], int(a.split(',')[3])))
aggregate = to_address.reduceByKey(lambda x,y: x+y)

def check_contract(line):
    try:
        fields = line.split(',')
        if len(fields) != 5:
            return False
        return True
    except:
        pass

contracts = sc.textFile("/data/ethereum/contracts")
good_contracts = contracts.filter(check_contract)
contract_address = good_contracts.map(lambda a: (a.split(',')[0], None))
newJoin = aggregate.join(contract_address)
top10 = newJoin.takeOrdered(10, key = lambda x: -x[1][0])


for record in top10:
    print("{}: {}".format(record[0],record[1]))








