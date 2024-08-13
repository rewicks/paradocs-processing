import sys

last_docid = None
scores = []

for line in sys.stdin:
    line = line.strip().split('\t')
    if last_docid is not None and line[0] != last_docid:
        doc_score = sum(scores) / len(scores)
        print(f"{last_docid}\t{doc_score}")
        scores = []
    try:
        scores.append(float(line[1]))
    except:
        print(line, file=sys.stderr)
        exit()
    last_docid = line[0]
