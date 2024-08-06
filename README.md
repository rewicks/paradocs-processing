# EXAMPLE RUN THROUGH

An example of how to use these scripts to re-create the en-nl data split.


## Downloading the data.

A key limitation is that this data alignment requires access to the monolingual data dump. In some cases, this is quite large (i.e., the English wide16 is 4T).
Be prepared to have significant data storage for this step and I recommend starting with the `marta` or `hieu` splits which are significantly smaller.


The `text` files contain base64 encoded versions of the documents while the `url` files contain the url from which the document was scraped.

### Monolingual Data

```
wget -c -4 -O "marta.eng.text.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/marta/eng/text.gz
wget -c -4 -O "marta.eng.url.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/marta/eng/url.gz

wget -c -4 -O "hieu.eng.text.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/hieu/eng/text.gz
wget -c -4 -O "hieu.eng.url.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/hieu/eng/url.gz

wget -c -4 -O "philipp.eng.text.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/philipp/eng/text.gz
wget -c -4 -O "philipp.eng.url.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/philipp/eng/url.gz

wget -c -4 -O "wide00006.eng.text.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/wide00006/eng/text.gz
wget -c -4 -O "wide00006.eng.url.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/wide00006/eng/url.gz

wget -c -4 -O "wide00016.eng.text.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/wide00016/eng/text.gz
wget -c -4 -O "wide00016.eng.url.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/wide00016/eng/url.gz


wget -c -4 -O "marta.nld.text.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/marta/nld/text.gz
wget -c -4 -O "marta.nld.url.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/marta/nld/url.gz

wget -c -4 -O "hieu.nld.text.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/hieu/nld/text.gz
wget -c -4 -O "hieu.nld.url.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/hieu/nld/url.gz

wget -c -4 -O "philipp.nld.text.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/philipp/nld/text.gz
wget -c -4 -O "philipp.nld.url.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/philipp/nld/url.gz

wget -c -4 -O "wide00006.nld.text.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/wide00006/nld/text.gz
wget -c -4 -O "wide00006.nld.url.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/wide00006/nld/url.gz

wget -c -4 -O "wide00016.nld.text.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/wide00016/nld/text.gz
wget -c -4 -O "wide00016.nld.url.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/wide00016/nld/url.gz
```

### Bitext Data

```
wget -c -4 -O https://web-language-models.s3.amazonaws.com/paracrawl/release9/en-nl/en-nl.classified.gz
```

The bitext is formatted as a tsv with each column meaning:

```
src_url tgt_url src_text    tgt_text    hash_one    hash_two    hash_three  similarity_one  similarity_two  collection_id

```

## Indexing the Monolingual Data

This is the most time-intensive part. For easy lookup, we iterate through the monolingual data line-by-line (which is document-by-document). We normalize the url and index based off of the normalized form. The idea is very simple but may burden simple file systems as it will generate many files...

More simply, each url will hash to a number. That number denoes the path of a file. Each individual file is relatively small, so searching for the document later based off the url is much cleaner.

The exact command would be:

```
paste <(unpigz -c marta.nld.url.gz) \
        <(unpigz -c marta.nld.text.gz) \
        | python -u scripts/split-urls --outdir marta-nld --prefix_length 7 --postfix 00
```

This makes the prefix length 7 which has a hash range of 0-->10^7. A url/document with a hash of 1234567 would be stored at `marta-nld/00/12/34/567.gz`. There's room for adjustments here based on better/more robust file systems. The postfix feature is designed so that you could parallelize the indexing (which is gonna generate more files but perhaps speeds this up). So during inference if I was looking for a file with the hash 1234567, I would look in all files `marta-nld/*/12/34/567.gz`. I used `prefix_length=7` for most monolingual file sizes and 8 for the larger English ones.

A small note: some of the monolingual data dumps (i.i.r.c `philipp` mostly) has many `unknown?=` type urls that get hashed to the same file. This makes one or more of the hashes very small and the url is entirely useless anyway. If the alignment step is very slow--it's worth printing out if it's getting stuck in one file and that likely is a garbage bin for these urls anyway. For my purposes, I deleted this entire file.

Repeat for each monolingual data dump.

## Sharding the bitext

I first shard the data into separate files based on collection.

```
zcat en-nl.classified.gz | python scripts/source-split sources/ en-nl
```

This will create a separate `.gz` file for each collection (i.e., `{marta,hieu,philipp,wide00006,wide00016}.en-nl.gz`) as well as a few collections for which there is not monolingual data (or is common crawl).

After this, I then shard each collection into ~100k line files for more parallelization.

```
unpigz -c hieu.en-nl.gz | python -u scripts/split-documents \
        --docfield 0 \
        --prefix docs/hieu \
        --chunk_size 100000
```

This will create files in a new directory called `docs/hieu`. You would do this for each collection (including the extraneous ones). Once everything is indexed and sharded, all of the alignment can happen in parallel (to the capacity of your system ;) ).

## Aligning the data


```
python -u scripts/align-docs --input docs/marta/000000.gz \
                            --mono_shard_dir mono/eng/marta \
                            --urlfield 0 \
                            --textfield 1 \
                            --lang en \
                            --prefix_length 8 \
                            --doc_out aligned/marta/eng/000000.docs.gz > aligned/marta/eng/000000.tmp


# Separate parameters for the non-English language. Note that the prefix length may be different. It is dependent on what you used to index the monolingual data. 
python -u scripts/align-docs --input docs/marta/000000.gz \
                            --mono_shard_dir mono/nld/marta \
                            --urlfield 1 \
                            --textfield 3 \
                            --lang nl \
                            --prefix_length 7 \
                            --doc_out aligned/marta/nld/000000.docs.gz > aligned/marta/nld/000000.tmp
```

I kept each column separate for my own debugging purposes so you may follow suit with something like:


```
cat aligned/marta/nld/000000.tmp | cut -f11 | gzip --best > aligned/marta/nld/000000.paragraph_id.gz
cat aligned/marta/nld/000000.tmp | cut -f12 | gzip --best > aligned/marta/nld/000000.sentence_id.gz
cat aligned/marta/nld/000000.tmp | cut -f13 | gzip --best > aligned/marta/nld/000000.start_index.gz
cat aligned/marta/nld/000000.tmp | cut -f14 | gzip --best > aligned/marta/nld/000000.end_index.gz
cat aligned/marta/nld/000000.tmp | cut -f15 | gzip --best > aligned/marta/nld/000000.docid.gz
```

And the same thing for the English data.

After all shards for a given collection/language pair is done, I would collapse into a file for each annotation:

```
cat aligned/marta/nld/{000000..000033}.paragraph_id.gz > aligned/marta/nld/marta.en-nl.nl.paragraph_id.gz
```
(33 is just an example--not sure how many shards there were for `marta/en-nl`.)

I would also recommend that `marta.en-nl.nl.paragraph_id.gz` has the same line count as `sources/marta.en-nl.gz`.

## Language Id

I have a wrapper script around NLLB's fasttext model that will output the probability for a given language, but very straightforward:

```
zcat docs/marta/000000.gz | cut -f3 | scripts/nllb_spec "__label__eng_Latn" | gzip --best > aligned/marta/eng/000000.langs.gz

# Column 4 for the target text
zcat docs/marta/000000.gz | cut -f4 | scripts/nllb_spec "__label__nld_Latn" | gzip --best > aligned/marta/nld/000000.langs.gz
```

## Frequency Counts
 
I do something similar as the monolingual indexing for the frequency counts.

```
zcat en-nl.classified.gz | cut -f3-4 | sort | uniq -c > sorted.en-nl
cat sorted.en-nl | sort -k 1 -n -r -T tmp/ | trim > sorted.counts.en-nl

# You don't need to index everything that only occurs once--that's assumed
cat sorted.counts.en-nl | grep -n "^1 " | head

# Let's assume the first 1 is found on line 123456789
cat sorted.counts.en-nl | sed -n '1,123456789p;12345679q' > sorted.counts.en-nl.trunc

python -u scripts/split-counts \
    --count_path sorted.counts.en-nl.trunc \
    --outdir counts/ \
    --prefix_length 7
```

Then you run each of the shards through the counts script. You only need to do this once per shard--unlike the others that require once per language per shard---since the count is determined by the source+target pair.

```
python -u scripts/assign-count.py --count_path counts \
                                                        --bitext_path docs/marta/000000.gz \
                                                        --prefix_length 7 \
                                                        > aligned/marta/eng/000000.counts.gz
```

## Extraneous Collections

We don't have monolingual data for the Common Crawl data, nor the `wide00015`, or `pdf`. I still run the frequency counts and the language id on these shards. Then for each of the annotation files, I create a file that is the same line length as the data with `None` on every line.

```
# Do this for each language
for annotation in {paragraph_id,sentence_id,start_index,end_index,docid}; do
    zcat source/cc-2016-30.en-nl.gz | awk '{print "None"}' | gzip > aligned/cc-2016-30/eng/$annotation.en-nl.en.gz;
done;

```

Now you can recombine all the data back together where it is precisely parallel with the original.

## Recombination

We recombine each file together into a single data.gz file which is parallel to the original ParaCrawl dataset. Something like:

```

# You can somewhat skip this if you feed these commands directly into paste
zcat en-nl.classified.gz | cut -f3 | gzip > src.gz
zcat en-nl.classified.gz | cut -f4 | gzip > tgt.gz
zcat en-nl.classified.gz | cut -f8 | gzip > similarity_one.gz
zcat en-nl.classified.gz | cut -f9 | gzip > similarity_two.gz
zcat en-nl.classified.gz | cut -f10 | gzip > collections.gz


# For each annotation type and language, you would want to recombine with something like:
cat aligned/cc-2016-30/eng/langs.en-nl.en.gz aligned/cc-2017-30/eng/langs.en-nl.en.gz ... aligned/philipp/eng/langs.en-nl.en.gz ... aligned/philipp/eng/langs.en-nl.en.gz > en-nl.en.langs.gz

# It is important that the above matches the original order of the collections in the classified file. You can double check with `zcat en-nl.classified | cut -f10 | uniq` 

paste <(zcat src.gz) \
        <(zcat tgt.gz) \
        <(zcat similarity_one.) \
        <(zcat similarity_two.gz) \
        <(zcat collections.gz) \
        <(zcat en-nl.en.paragraph_id.gz) \
        <(zcat en-nl.nl.paragraph_id.gz) \
        <(zcat en-nl.en.sentence_id.gz) \
        <(zcat en-nl.nl.sentence_id.gz) \
        <(zcat en-nl.en.start_index.gz) \
        <(zcat en-nl.en.end_index.gz) \
        <(zcat en-nl.nl.start_index.gz) \
        <(zcat en-nl.nl.en_index.gz) \
        <(zcat en-nl.en.langs.gz) \
        <(zcat en-nl.nl.langs.gz) \
        <(zcat en-nl.en.counts.gz) \
        <(zcat en-nl.en.docid.gz) \
        <(zcat en-nl.nl.docid.gz) \
        | gzip > paradocs.en-nl.gz
```

## Extracting data

Using the datascripts I have in the public [repo](https://github.com/rewicks/ParaDocs/blob/main/paradocs/paradocs), you can filter out sentences as described in the [paper](https://arxiv.org/pdf/2406.03869) Section 3.2.

```
unpigz -c paradocs.en-nl.gz | python paradocs --minimum_size 2 --frequency_cutoff 100 --lid_cutoff 0.5 --min_avg_score 0.0 > data
```

This is equivalent to the `Docs` split described in the paper.

## Filtering

We use a slide-based input to a QE model. You can make the inputs from the sliding window using:

```
cat data | cut -f1-3 | python scripts/make-input.py --window 3 --stride 1 > inputs
```

Then to score using comet-kiwi (you probably want to shard this too):
```
cat inputs | cut -f2  > src
cat inputs | cut -f3  > tgt

comet-score -s src -t tgt --model Unbabel/wmt22-cometkiwi-da > scores
```

Then we get the document level scores by averaging all scores from a given document:
```
paste <(inputs | cut -f1) <(cat scores | grep "Segment" | cut -f3 | sed 's/score: //g') | python scripts/get-doc-scores.py > doc-scores 
```

Then we sort the scores and we could choose to only keep the top 10k documents:

```
cat doc-scores | sort -k 2 -n -r > sorted.doc-scores
cat sorted.doc-scores | head -10000 > docs-to-keep
```

We can then filter from our original data set to create a filtered subset:

```
cat data | python scripts/keep.py docs-to-keep > filtered-data
```
