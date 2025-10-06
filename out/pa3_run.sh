#!/usr/bin/env bash
# PA3 End-to-End Script 
# Usage: bash pa3_run.sh
set -euo pipefail

# Build edges, from dataset, enity count and thresholded edges
cut -d',' -f3,2 dataset_clean.csv | tail -n +2 | tr ',' '\t' \
| awk -F'\t' '{print $2 "\t" $1}' \
| awk -F'\t' '{
    track_id = $2
    split($1, artists, ";")
    for (i in artists) {
        gsub(/^[ \t]+|[ \t]+$/, "", artists[i])
        if (artists[i] != "")
            print artists[i] "\t" track_id
    }
}' | sort -u > edges.tsv

cut -f1 edges.tsv | sort | uniq -c | sort -nr > entity_counts.tsv

awk '$1 >= 50 {$1=""; print substr($0,2)}' entity_counts.tsv > significant_entities.txt

awk -F'\t' 'NR==FNR{entities[$0]=1; next} $1 in entities' \
    significant_entities.txt edges.tsv > edges_thresholded.tsv

rm significant_entities.txt

# Histogram of cluster sizes, top 30 overall, clusters and diff
cut -f1,2 edges_thresholded.tsv | sort -u | cut -f1 | uniq -c | sort -nr \
| sed -E 's/^[[:space:]]*([0-9]+)[[:space:]]+(.+)/\2\t\1/' > cluster_sizes.tsv

cut -d',' -f21 dataset_clean.csv | tail -n +2 | sed 's/.*/\L&/' | sed '/^$/d' \
| sort | uniq -c | sort -nr | head -30 > top30_overall.txt

cut -f2 edges_thresholded.tsv | grep -F -f - dataset_clean.csv \
| cut -d',' -f21 | sed 's/.*/\L&/' | sed '/^$/d' \
| sort | uniq -c | sort -nr | head -30 > top30_clusters.txt

paste top30_overall.txt top30_clusters.txt > diff_top30.txt


# Create cluster_viz.png

TOP_LEFT="$(cut -f1 edges_thresholded.tsv | sort | uniq -c | sort -nr | head -1 | awk '{$1=""; sub(/^ +/,""); print}')"
awk -v a="$TOP_LEFT" -F'\t' '$1==a {print $1 "\t" $2}' edges_thresholded.tsv > _ego_edges.tsv

{
  echo 'graph G {'
  echo '  graph [splines=true];'
  echo '  node  [shape=circle, fontsize=8];'
  echo '  edge  [penwidth=0.6];'
  printf '  "%s" [shape=ellipse, fontsize=12, penwidth=1.2, style=filled];\n' "$TOP_LEFT"
  awk -F'\t' '{gsub(/"/,"",$1); gsub(/"/,"",$2); printf("  \"%s\" -- \"%s\";\n",$1,$2)}' _ego_edges.tsv
  echo '}'
} > cluster.dot

if command -v sfdp >/dev/null 2>&1; then
  sfdp -x -Tpng cluster.dot -o cluster_viz.png
elif command -v neato >/dev/null 2>&1; then
  neato -n2 -Tpng cluster.dot -o cluster_viz.png
elif command -v dot >/dev/null 2>&1; then
  dot -Kneato -n2 -Tpng cluster.dot -o cluster_viz.png
else
  echo "Graphviz not found (sfdp/neato/dot)."
fi

rm -f _ego_edges.tsv cluster.dot




# Summary statistics using datamash
ti=$(head -1 "$CSV" | awk -v FPAT='([^,]*)|(\"([^\"]|\"\")*\")' '{
  for(i=1;i<=NF;i++){x=$i; gsub(/"/,"",x); if(x=="track_id") T=i; if(x=="energy") E=i}
  if(T&&E) print T; else exit 1 }') || { echo "track_id/energy headers not found"; exit 1; }
ei=$(head -1 "$CSV" | awk -v FPAT='([^,]*)|(\"([^\"]|\"\")*\")' '{
  for(i=1;i<=NF;i++){x=$i; gsub(/"/,"",x); if(x=="track_id") T=i; if(x=="energy") E=i}
  if(T&&E) print E; else exit 1 }')

awk -v FPAT='([^,]*)|(\"([^\"]|\"\")*\")' -v TI="$ti" -v EI="$ei" '
  NR>1 {id=$TI; en=$EI; gsub(/"/,"",id); gsub(/"/,"",en);
        gsub(/^ +| +$/,"",id); gsub(/^ +| +$/,"",en);
        if(id!="" && en!="") print id "\t" en }' "$CSV" \
| LC_ALL=C sort -u > freq_energy.full.tsv

awk -F'\t' 'NR>1 || $0!~/^(Left|Artist|Entity)/{print $1 "\t" $2}' edges_thresholded.tsv > edges_nohdr.tsv

awk -F'\t' 'NR==FNR{m[$1]=$2; next} ($2 in m){print $1 "\t" m[$2]}' \
  freq_energy.full.tsv edges_nohdr.tsv > left_outcome.tsv

LC_ALL=C sort -t $'\t' -k1,1 left_outcome.tsv > left_outcome.sorted.tsv

datamash -t $'\t' -g 1 count 2 mean 2 median 2 < left_outcome.sorted.tsv \
| awk 'BEGIN{print "LeftEntity\tcount\tmean_energy\tmedian_energy"}1' \
> cluster_outcomes.tsv


