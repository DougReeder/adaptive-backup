#!/bin/sh
find . -type f -ls | awk '
{
    type = $11;
    if ( type ~ /\./ ) {
        sub(/^.*\./, "", type);
    } else {
        type = ".";
    }
    sizes[type] += $7;
}
END {
    for ( type in sizes ) {
        printf "%10d %s\n", sizes[type], type;
    }
}' | sort -r -n
