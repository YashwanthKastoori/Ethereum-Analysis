#Import the MRJob class from mrjob python library
from mrjob.job import MRJob


#Declare the class cw, that extends the MRJob format.
class cw(MRJob):

    # Declaration of the mapper, getting line as input
    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if (len(fields)==7):
                #extract the to_address info
                to_address = fields[2]
                #Extract the value
                value = int(fields[3])
                # Yield the pair {to_address,value}
                yield (to_address,value)
        except:
            pass


    #Declaration of the Combiner
    def combiner(self, address, values):
        # Define a variable "total" to add all counts
        total = sum(values)
        # Yield only the non-zero values
        if total>0:
            # Yield the pair {address,total}
            yield (address, total)

    #Declaration of the reducer
    def reducer(self, address, values):
        # Define a variable "total" to add all counts
        total = sum(values)
        # Yield the pair {address,total}
        yield (address, total)

#Run the defined MapReduce job.

if __name__ == '__main__':
    cw.run()
