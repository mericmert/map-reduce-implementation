import sys
from FindCyclicReferences import FindCyclicReferences
from FindCitations import FindCitations
def main(argv, argc):

    if argc != 4:
        sys.exit(1)
    command, num_of_workers, file_name = argv[1], int(argv[2]), argv[3]

    if command == "COUNT":
        mr = FindCitations(num_of_workers)
        mr.start(file_name)
    elif command == "CYCLE":
        mr = FindCyclicReferences(num_of_workers)
        mr.start(file_name)

if __name__ == "__main__":
    main(sys.argv, len(sys.argv))

