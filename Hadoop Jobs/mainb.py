#Import the MRJob class from mrjob python library
from mrjob.job import MRJob

#Declare the class cw, that extends the MRJob format.
class cw(MRJob):

    # Declaration of the mapper, getting line as input
    def mapper(self, _, line):
        try:
            fields = line.split("\t")

            if (len(fields)==2):
                #Extract the Address
                address = fields[0]
                cleanAddress = address.replace('"','')
                value = int(fields[1])
                yield ("Line", (cleanAddress, value))

        except:
            pass

    #Declaration of the reducer
    def reducer(self, key, unsorted_values):
        values = sorted(unsorted_values, reverse=True, key=lambda item: item[1])
        # Loop over the  first 10 elements of the "Values" array
        for value in list(values)[0:10]:
            #  Yield the pair {address,total}
            yield (value[0], value[1])

#Run the defined MapReduce job.

if __name__ == '__main__':
    cw.run()
