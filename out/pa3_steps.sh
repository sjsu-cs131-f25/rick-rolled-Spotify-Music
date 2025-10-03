#!/usr/bin/env bash
# PA3 End-to-End Script
# Usage: bash pa3_steps.sh

set -euo pipefail

# Build edges from dataset, enity count and thresholded edges
cut -d',' -f3,2 dataset_clean.csv | tail -n +2 | tr ',' '\t' | \

awk -F'\t' '{print $2 "\t" $1}' | \

awk -F'\t' '{
            track_id = $2
            split($1, artists, ";")
            for(i in artists) {
                        gsub(/^[ \t]+|[ \t]+$/, "", artists[i])
                        print artists[i] "\t" track_id
            }
}' | sort -u > edges.tsv

cut -f1 edges.tsv | sort | uniq -c | sort -nr > entity_counts.tsv

awk '$1 >= 50 {$1=""; print substr($0,2)}' entity_counts.tsv > significant_entities.txt

awk -F'\t' 'NR==FNR{entities[$0]=1; next} $1 in entities' \
  significant_entities.txt edges.tsv > edges_thresholded.tsv

rm significant_entities.txt

# Histogram of cluster sizes, top 30 overall, clusters and diff top 30

cut -f1,2 edges_thresholded.tsv | sort -u | cut -f1 | uniq -c | sort -nr \
| sed -E 's/^[[:space:]]*([0-9]+)[[:space:]]+(.+)/\2\t\1/' > cluster_sizes.tsv

cut -d',' -f21 dataset_clean.csv | sort | uniq -c | sort -nr | head -30 > top30_overall.txt

cut -f2 edges_thresholded.tsv | grep -F -f - dataset_clean.csv | cut -d',' -f21 | sed 's/.*/\L&/' | sed '/^$/d' | sort | u$

paste top30_overall.txt top30_clusters.txt > diff_top30.txt

# Summary statistics(datamash)
tail -n +2 freq_energy.txt | awk '{print $1 "\t" $2}' > freq_energy.tsv
tail -n +2 edges_thresholded.tsv > edges_nohdr.tsv

awk -F'\t' 'NR==FNR{m[$1]=$2; next} {if ($2 in m) print $1 "\t" m[$2]}' \
  freq_energy.tsv edges_nohdr.tsv > left_outcome.tsv

LC_ALL=C sort -t $'\t' -k1,1 left_outcome.tsv > left_outcome.sorted.tsv

datamash -t $'\t' -g 1 count 2 mean 2 median 2 \
  < left_outcome.sorted.tsv > cluster_outcomes.tsv

echo "[PA3] Finished"
