from mrjob.job import MRJob
import re

#DOB : 31 JULY,2002.

class Words(MRJob):

    def mapper(self, _, line):
        all_words = re.findall(r'\b[a-zA-Z]+\b', line.lower())

        for word in all_words:
            yield word, 1

    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == '__main__':
    Words.run()
