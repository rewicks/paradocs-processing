import sys

doc_file=sys.argv[1]

docids = set()
with open(doc_file) as infile:
    for line in infile:
        docids.add(line.strip().split('\t')[0])

for line in sys.stdin:
    docid = line.strip().split('\t')[0] 
    if docid in docids:
        print(line.strip())
