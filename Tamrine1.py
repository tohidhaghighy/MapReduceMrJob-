from mrjob.job import MRJob
from mrjob.step import MRStep

class MRWordFrequencyCount(MRJob):

    def mapper_first(self, key, line):
        a=[]
        a=line.split(',')
        val=float(a[2])
        yield a[1],val
            

    def reducer_first(self, key, values):
        yield "1",(int(key),sum(values)/50)
    
    def reducer_second(self, key, values):
        for key,avg in sorted(values, reverse=False):
            yield key,avg

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