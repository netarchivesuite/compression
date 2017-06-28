#!/usr/bin/env bash

print_usage()
{
    echo "Usage: $(basename $0) -d [-t <tmpdir>] -o <outputfile> dir"
    echo
    echo "Recursively merges/sorts files under dir"
    echo
    echo "-d delete source files"
    echo "-t tempdir for sorting"
    echo "-o output file"
}

delete=0
tempdir=$(dirname $(mktemp -u))
while  getopts ":t:o:d" opt; do
    case "$opt" in
       d)
          delete=1
          ;;
       t)
          tempdir="$OPTARG"
          ;;
       o)
          output="$OPTARG"
          ;;
       ?)
          print_usage
          exit 1
          ;;
    esac
done

shift $(($OPTIND - 1))
dir=$1

[ -z "$dir" ] && print_usage && exit 1
[ -z "$output" ] && print_usage && exit 1

tempfile=$(mktemp)
echo "Sorting files in $dir to $output, using temporary file $tempfile."

find $dir -type f -exec bash -c "cat '{}' >>$tempfile; if [ $delete -eq 1 ]; then rm '{}'; fi" \;
LC_ALL_C sort -T $tempdir $tempfile > $output
rm $tempfile

