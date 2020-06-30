#Import the MRJob class from mrjob python library
from mrjob.job import MRJob


#Declare the class cw, that extends the MRJob format.
class cw(MRJob):

    # Declaration of the mapper, getting line as input
    def mapper(self, _, line):
        #repartition join between the aggregate and contracts
        try:  
            fields = line.split("\t")
            fields2 = line.split(",")
            if (len(fields)==2):
                #Extract the Address
                address = fields[0]
                join_key = address.replace('"','')
                #Extract the Value
                join_value = fields[1]
                #Yield de adress as a key, and the pair (value, 1)
                #The 1 in the pair (value, 1) will be used in the reducer
                yield (join_key, (join_value, 1))

            elif (len(fields2)==5):
                # this is a Contract for the join
                join_key = fields2[0]
                # Yield de address as a key, and the pair ("", 2)
                # The 2 in the pair ("", 2) will be used in the reducer
                yield (join_key, ("", 2))

        except:
            pass

    #Declaration of the reducer
    def reducer(self, address, values):
        #count the joints results
        counter = 0
        #Total value of the Transaction
        totalValue = 0

        for value in values:
            if value[1] == 1:
                #We have an Aggregate
                counter+=1
                #Extract the Value of the Transaction
                totalValue=int(value[0])
            elif value[1] == 2:
                # We have an contract
                counter+=1

        # After the previous loop if counters is larger than 1
        # We have found a transaction with at least one contract
        # So it is in fact a smart-contract
        if (counter > 1):
            # Yield de address and the total Value  {address,total}
            yield (address,totalValue)


#Run the defined MapReduce job.

if __name__ == '__main__':
    cw.run()
