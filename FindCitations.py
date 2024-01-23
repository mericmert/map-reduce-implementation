from MapReduce import MapReduce

class FindCitations(MapReduce):

    def __init__(self, num_workers: int):
        super().__init__(num_workers)

    def map(self, chunk):
        res = {}
        for line in chunk:
            source, target = line.split("\t")
            if target not in res:
                res[target] = 0
            res[target] += 1
        return res


    def reduce(self, results):
        reduced_result = {}
        for citation_count in results:
            for item, count in citation_count.items():
                if item not in reduced_result:
                    reduced_result[item] = 0
                reduced_result[item] += count

        return reduced_result

