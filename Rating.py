from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
class MRRating(MRJob):
INPUT_PROTOCOL = RawValueProtocol
def steps(self):
    return [
        MRStep(mapper=self.mapper,
            reducer=self.reducer)
]
def mapper(self, _, line):
data = line.split(';')
if data[0] != "User-ID":
    yield data[1],float( data[2])