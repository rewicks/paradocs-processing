import sys
import argparse

def count_tokens(line, spm):
    return len(spm.encode(line) if spm else line.split())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--window", default=10, type=int)
    parser.add_argument("--stride", default=10, type=int)
    args = parser.parse_args()

    last_docid = None
    document = []
    for line in sys.stdin:
        line = line.strip().split('\t')
        if line[0] != last_docid and last_docid is not None:
            for i in range(0, max(len(document)-args.window, 1), args.stride):
                srcs = " ".join([d[0] for d in document[i:i+args.window]])
                tgts = " ".join([d[1] for d in document[i:i+args.window]])
                print(f'{last_docid}\t{srcs}\t{tgts}')
            document = []
        last_docid = line[0]
        if len(line) > 2:
            document.append((line[1].strip(), line[2].strip()))
        else:
            document.append((line[1].strip(), ""))
