from mrjob.job import MRJob
from mrjob.step import MRStep
import math

class MRWordFrequencyCount(MRJob):

#M*N
    def mapper_first(self, key, line):
        a=[]
        a=line.split(',')
        if a[0]=="M":
            yield a[2],("M",a[1],a[3])
        if a[0]=="N":
            yield a[1],("N",a[2],a[3])
        
            

    def reducer_first(self, key, values):
        arrvalN=[]
        arrvalM=[]
        for matrix,colum,val in values:
            if  matrix=="N":
                arrvalN.append({"key":colum,"value":val})
            if matrix=="M":
                arrvalM.append({"key":colum,"value":val})
                   
        for s in arrvalN:
            for ss in arrvalM:
                yield (ss["key"],s["key"]),((float(s["value"]))*(float(ss["value"])))

    def reducer_second(self, key, values):
        sum=0
        for val in values:
            sum+=val
        yield key,sum

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_first,
                reducer=self.reducer_first
            ),
            MRStep(
                reducer=self.reducer_second
            )
        ]
    
    
if __name__ == '__main__':
    MRWordFrequencyCount.run()