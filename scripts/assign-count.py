import argparse
import sys
import gzip
import os
import xxhash
import math

class HashXX32(object):
    """
        Class to hash the ngrams. They are seeded sequentially for consistency.
    """
    def __init__(self, seed : int, max_hash_value : int):
        self.h = xxhash.xxh32(seed=seed)
        self.max_hash_value = max_hash_value

    def hash(self, o : str):
        """
            Hashes input string and offsets value based on offset (reserved space in the embedding space)
        """
        self.h.reset()
        self.h.update(o)
        
        hash_value = self.h.intdigest() % self.max_hash_value
        return str(hash_value)





def find_count(hasher, sentence, prefix_length, base_dirs):
    sentence[0] = sentence[0].replace(" ", "")
    sentence[1] = sentence[1].replace(" ", "")
    prefix = hasher.hash(f"{sentence[0]} {sentence[1]}").zfill(prefix_length)

    for base_dir in base_dirs:
        count_dir = os.path.join(base_dir, f"{prefix[0]}{prefix[1]}/{prefix[2]}{prefix[3]}")

        count_path = os.path.join(count_dir, f"{prefix[4:]}.gz")

        if os.path.exists(count_path):
            with gzip.open(count_path, 'rt') as infile:
                for i, line in enumerate(infile):
                    line = line.strip().split('\t')
                    try:
                        count = line[0]
                        src = line[1].split()[0]
                        tgt = line[1].split()[1]
                        if sentence[0] == src and sentence[1] == tgt:
                            return int(count)
                        if int(count) == 1:
                            return 1
                    except:
                        return 1
    return 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count_path")
    parser.add_argument("--bitext_path")
    parser.add_argument("--prefix_length", type=int, default=7)

    args = parser.parse_args()
    print(args.bitext_path, file=sys.stderr)

    prefix_dirs = []
    for _, si, _ in os.walk(args.count_path):
        for s in si:
            prefix_dirs.append(os.path.join(args.count_path, s))
        break
        
    print(prefix_dirs, file=sys.stderr)

    hasher = HashXX32(seed=14, max_hash_value=int(math.pow(10, args.prefix_length)))
    with gzip.open(args.bitext_path, 'rt') as bfile:
        for i, line in enumerate(bfile):
            line = str(line).strip().split('\t')
            count = find_count(hasher, [line[2], line[3]], args.prefix_length, prefix_dirs)
            print(count)

