#!/bin/bash
set -e

for out_dir in static dynamic; do
    if [ ! -d "$out_dir" ]; then
        echo "Directory $out_dir does not exist, skipping."
        continue
    fi

    for dir in "$out_dir"/*/; do
        if compgen -G "$dir"*.csv.gz > /dev/null; then
            echo "Processing directory: $dir"
            output="${dir%/}/merged.csv"
            first=1

            > "$output"

            for file in "$dir"*.csv.gz; do
                if [ $first -eq 1 ]; then
                    gunzip -c "$file" >> "$output"
                    first=0
                else
                    gunzip -c "$file" | tail -n +2 >> "$output"
                fi
                echo "  Merged $file"
            done

            echo " output: $output"
        else
            echo "  skip dir: $dir (no .csv.gz files found)"
        fi
    done
done

