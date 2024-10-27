from mrjob.job import MRJob
from mrjob.step import MRStep

class Language_Budget_Countries(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]
    
    def mapper(self, _, line):
        columns = line.split('|')
        try:
            language = columns[2].strip()
            country = columns[3].strip()
            budget = float(columns[4].strip())
            
            if language and country and budget > 0:
                yield language, (country, budget)
        except (IndexError, ValueError):
            pass  
    
    def reducer(self, language, values):
        country_set = set()  
        total_budget = 0
        
        for country, budget in values:
            country_set.add(country)
            total_budget += budget
        
        country_list = list(country_set)
        yield language, [country_list, int(total_budget)]

if __name__ == '__main__':
    Language_Budget_Countries.run()