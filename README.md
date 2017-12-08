# Pre-Compression Algorithm

This is a fairly simple algorithm. We use jwat to create a compressed version of the input file. The file is then 
decompressed to check that we get exactly the same bits back. We then create two cdx iterators, one for each file, and
 run through them in parallel. The iterator for the compressed file is used to generate the new basis cdx files. The
 two results from the two iterators are combined to create the ifile, in which each line consists of
 <old cdx offset> <new cdx offset> <old cdx harvest timestamp>
Any error will result in no more cdx output being written to either file, but for some reason only IOException and 
NullPointerException are caught and logged with a "Problem indexing files" message.

# Metadata Migration

This is quite a complex algorithm because

1. It has to migrate "duplicate:" annotations, using ifiles from possibly many different other files
1. It has to migrate metadata-cdx records using a single ifile per (w)arc-record
1. It creates two new records:
    1. A lookup table for migrating all the old duplicate records so the process is reproducible, and
    1. A cdx index of the migrated duplicate records
1. This all has to be done on-disk, not in-memory because it has to support very large records, and
1. Everything has to support both arc and warc input files

# Compression

Scripts related to the workflow for compressing an arc/warc repository. 

Compression tasks are carries out with [JWAT-Tools](https://sbforge.org/display/JWAT/JWAT).  