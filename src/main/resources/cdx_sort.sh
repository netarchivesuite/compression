#!/usr/bin/env bash

print_usage()
{
    echo "Usage: $(basename $0) -d [-t <tmpdir>] [-e <regex>] -o <outputfile> dir1 dir2 ..."
    echo
    echo "Recursively merges/sorts files under dir"
    echo
    echo "-d delete source files"
    echo "-t tempdir for sorting"
    echo "-e regex matching filenames to be included, e.g. -e '1[0-4]*' (default value '*')"
    echo "-o output file"
    echo
    echo "Example: ./cdx_sort.sh -t /tmp -e '*.txt' -o sorted.txt mydir yourdir"
}

delete=0
regex='*'
tempdir=$(dirname $(mktemp -u))
while  getopts ":t:o:e:d" opt; do
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
       e)
          regex="$OPTARG"
          ;;
       ?)
          print_usage
          exit 1
          ;;
    esac
done

shift $(($OPTIND - 1))
dirs=$@

[ -z "$dirs" ] && print_usage && exit 1
[ -z "$output" ] && print_usage && exit 1

tempfile=$(mktemp)
echo "Sorting files in $dirs matching $regex to $output, using temporary file $tempfile."

find $dirs -type f -name "$regex" -exec bash -c "cat '{}' >>$tempfile; if [ $delete -eq 1 ]; then rm '{}'; fi" \;
LC_ALL=C sort -T $tempdir $tempfile > $output
rm $tempfile

