from pyspark.sql import SparkSession
from spellchecker import SpellChecker
import re

class NonEnglishWords(MRJob):
    def mapper(self, _, line):
        spell = SpellChecker()
        
        words = re.findall(r'\b\w+\b', line.lower())
        
        non_english= [word for word in words if word.lower() not in spell]

        for word in non_english:
            yield word, 1

    def reducer(self, word, counts):
        total_count = sum(counts)
        yield word, total_count

if __name__ == '__main__':
    NonEnglishWords.run()
