from MapReduce import MapReduce

class FindCyclicReferences(MapReduce):

    def __init__(self, num_workers: int):
        super().__init__(num_workers)

    def map(self, chunk):
        citations = {}
        for node in chunk:
            source, target = node.split("\t")
            if source not in citations:
                citations[source] = []

            citations[source].append(target)

        return citations

    def reduce(self, results):
        global_references = {}
        reduced_result = {}
        for dic in results:
            for source, references in dic.items():
                if source not in global_references:
                    global_references[source] = []
                global_references[source] += references

        for source, references in global_references.items():
            for reference in references:
                if reference in global_references and source in global_references[reference]:
                    if source < reference:
                        reduced_result[str((source, reference))] = 1
                    else:
                        reduced_result[str((reference, source))] = 1

        return reduced_result
