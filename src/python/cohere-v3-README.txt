
Vectors (.vec files) and
wiki_id,paragraph_count,paragraph_id,title,text,url (.csv files) for
Cohere's v3 wikipedia en vectors from HuggingFace:
https://huggingface.co/datasets/Cohere/wikipedia-2023-11-embed-multilingual-v3

See tools/python/load_cohere_v3.py (and shuffle_wiki_ids.py) for how
these luceneutil corpus files were downloaded/extracted/produced.
These tools were written with Claude Code's help and they are a little
bit Frankenstein as a result -- either because of my (Mike M)
ineptitude in prompting/iterating, or the crazy mistakes/approaches
Claude chose to take and I didn't fix, etc.

We download just language=en, and split=train (the only split,
apparently) from HuggingFace.

Notes:
  * Vectors are 1024 dimensions (up from 768 in Cohere v2), float32, and
    seem to be unit-sphere normalized (at least the first 10 vectors are).
  * Each wiki page (wiki_id primary key) has multiple paragraphs, and
    each paragraph has a vector
  * There are 5,854,887 wiki pages, with total 41,488,110 paragraphs -- ~7.1
    paragraphs per page on average
  * First we shuffle the input wiki_ids (so we are not vulnerable to
    corpus-order-bias -- see https://github.com/mikemccand/luceneutil/issues/494) 
  * Then we pick 250,000 random wiki_ids as "queries", and the rest
    are "docs" (5,604,887 wiki_ids).  That yields 1,778,739 query
    vectors (39,709,371 doc vectors).
  * No vectors are in common between docs and queries
  * The .vec files are in luceneutil's simple format: one vector after
    another written in binary (4 IEEE bytes little endian for float32)
  * The .csv files correspond row-by-row with the .vec files, once you
    read the initial header
  * The -coalesced- csv/vec files means all paragraphs appear under
    their wiki_id (for testing multi-valued vector search e.g. parent join)
  * The -scattered- csv/vec files means the paragraphs/vectors were
    shuffled away from their owning wiki_id.

Copy these csv/vec files to your dev workspace using initial_setup.py
(or just get the URLs from there and use wget/curl/etc. to download
locally).
