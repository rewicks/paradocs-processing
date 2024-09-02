echo "Downloading lid model"
wget -O "scripts/lid.model" https://dl.fbaipublicfiles.com/nllb/lid/lid218e.bin

echo "Making a directory for the raw files"
mkdir -p raw

echo "Downloading (Maltese) monolingual data files..."
for collection in {marta,hieu,philipp,wide00006,wide00016}; do
    echo "Downloading $collection..."
    wget -c -4 -O "raw/$collection.mlt.text.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/$collection/mlt/text.gz
    wget -c -4 -O "raw/$collection.mlt.url.gz" https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/monolingualv9/$collection/mlt/url.gz
done

echo "Downloading bitext"
wget -O "raw/en-mt.classified.gz" https://web-language-models.s3.amazonaws.com/paracrawl/release9/en-mt/en-mt.classified.gz

echo "Indexing monolingual data files..."
for collection in {marta,hieu,philipp,wide00006,wide00016}; do
    echo "Indexing $collection..."
    mkdir -p index/$collection/mt
    paste <(zcat raw/$collection.mlt.url.gz) <(zcat raw/$collection.mlt.text.gz) | python -u scripts/split-urls --outdir index/$collection/mt --prefix_length 7 --postfix 00
done;

echo "Split the bitext into collections..."
mkdir -p sources/en-mt
zcat raw/en-mt.classified.gz | python scripts/source-split sources/en-mt/ en-mt


echo "Sharding collections into smaller files.." 
for collection in {marta,hieu,philipp,wide00006,wide00016}; do
    echo "Sharding $collection..."
    mkdir -p docs/en-mt/$collection
    cat sources/en-mt/$collection.en-mt | python -u scripts/split-documents --docfield 0 --prefix docs/en-mt/$collection/ --chunk_size 1000000
done;


echo "Getting frequency counts..."
echo "Sorting..."
mkdir -p sorting/en-mt
zcat raw/en-mt.classified.gz | cut -f3-4 | sort | uniq -c > sorting/en-mt/sorted.en-mt

echo "Trimming counts..."
mkdir -p sorting/en-mt/tmp
cat sorting/en-mt/sorted.en-mt | sort -k 1 -n -r -T sorting/en-mt/tmp/ | trim > sorting/en-mt/sorted.counts.en-mt

echo "Truncating counts..."
cat sorting/en-mt/sorted.counts.en-mt | grep -v "^1 " > sorting/en-mt/sorted.counts.en-mt.trunc

echo "Indexing counts..."
mkdir -p counts/en-mt/00
python -u scripts/split-counts \
    --count_path sorting/en-mt/sorted.counts.en-mt.trunc \
    --outdir counts/en-mt/00 \
    --prefix_length 7

rm -r sorting/en-mt

echo "Aligning the shards..."
mkdir -p meta/en-mt/mt
for collection in {marta,hieu,philipp,wide00006,wide00016}; do
    echo "Aligning $collection..."
    mkdir -p meta/en-mt/mt/$collection
    for shard in docs/en-mt/$collection/*; do
        echo "Aligning $shard..."
        shard_num=$(echo $shard | rev | cut -d'/' -f1 | cut -d'.' -f2 | rev);
        python -u scripts/align-docs --input $shard \
                                    --mono_shard_dir index/$collection/mt \
                                    --urlfield 1 \
                                    --textfield 3 \
                                    --lang mt \
                                    --prefix_length 7 \
                                    --doc_out meta/en-mt/mt/$collection/$shard_num.docs.gz > meta/en-mt/mt/$collection/$shard_num.tmp

        cat meta/en-mt/mt/$collection/$shard_num.tmp | cut -f11 | gzip --best > meta/en-mt/mt/$collection/$shard_num.paragraph_id.gz
        cat meta/en-mt/mt/$collection/$shard_num.tmp | cut -f12 | gzip --best > meta/en-mt/mt/$collection/$shard_num.sentence_id.gz
        cat meta/en-mt/mt/$collection/$shard_num.tmp | cut -f13 | gzip --best > meta/en-mt/mt/$collection/$shard_num.start_index.gz
        cat meta/en-mt/mt/$collection/$shard_num.tmp | cut -f14 | gzip --best > meta/en-mt/mt/$collection/$shard_num.end_index.gz
        cat meta/en-mt/mt/$collection/$shard_num.tmp | cut -f15 | gzip --best > meta/en-mt/mt/$collection/$shard_num.docid.gz
         
        echo "Running langid on $shard"
        zcat $shard | cut -f4 | scripts/nllb_spec "__label__mlt_Latn" scripts/lid.model | gzip --best > meta/en-mt/mt/$collection/$shard_num.langs.gz
    
        echo "Assigning count to $shard..."
        python -u scripts/assign-count.py --count_path counts/en-mt/ \
                                            --bitext_path $shard \
                                            --prefix_length 7 \
                                            | gzip --best > meta/en-mt/mt/$collection/$shard_num.counts.gz
    done;
done;
