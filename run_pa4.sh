#!/usr/bin/env bash

#idk why but if i put the pipefail then it wont let me go past the before_sample.csv yall

# mkdir -p out

# 1) Clean & normalize (SED)

# grep -vi '".*,.*"' dataset.csv > out/dataset_clean.csv
echo "Step 1"
echo "cleaning/normalizing..."
sed -E '
	s/^[[:space:]]+//;			# trim leading spaces
	s/[[:space:]]+$//;			# trim trailing spaces
	s/[[:space:]]*,[[:space:]]*/,/g; 	# rm spaces around commas
	s/[][{}()]//g;				# rm brackets ()[]{}
' out/dataset_clean.csv > cleaned.csv

# ensure rows have same num of cols as header
awk -F, 'NR==1 {col=NF; print; next} NF==col' cleaned.csv > out/dataset_consistent.csv

echo "saved dataset_consistent.csv to out/"

rm cleaned.csv

echo "getting before sample..."
cat data/dataset.csv | head > out/before_sample.csv
echo "saved before_sample.csv to out/"

echo "getting after sample..."
cat out/dataset_consistent.csv | head > out/after_sample.csv
echo "saved after_sample.csv to out/"

# 2) Skinny Table and Frequency Table
echo "Step 2"

{
	echo -e "count\talbum_name"
	tail -n +2 out/dataset_consistent.csv | cut -d ',' -f4 | sort | uniq -c | sort -nr
} > out/freq_album_name.txt

echo "saved freq_album_name.txt to out/"

{
	echo -e "count\tduration_ms"
	tail -n +2 out/dataset_consistent.csv | cut -d ',' -f7 | sort | uniq -c | sort -nr
} > out/freq_duration.txt

echo "saved freq_duration.txt to out/"

{
	echo -e "count\tkey"
	tail -n +2 out/dataset_consistent.csv | cut -d ',' -f11 | sort | uniq -c | sort -nr | head -n 5
} > out/top5_keys.txt
echo "saved top5_keys.txt to out/"

{
	echo -e "popularity\tspeechiness\tinstrumentalness"
	tail -n +2 out/dataset_consistent.csv | cut -d ',' -f6,14,16 | sort -t',' -nrk1,1 | sed 's/,/\t/g'
} > out/pop_speech_instr.txt

echo "saved pop_speech_instr.txt to out/"

# 3,4) Quality Filtering, Buckets, Ratios and Per-Entity Summaries
echo "Step 3"
echo "Step 4"
mkdir -p logs
awk -f step34.awk out/dataset_consistent.csv

# 5) Temporal or string structure
echo "Step 5"
awk -F, '
NR > 1 {
	len = length($5);
	if (len <= 20) bucket = "Short";
	else if (len <= 50) bucket = "Medium";
	else bucket = "Long";
freq[bucket]++;
}
END {
	print "Bucket,Frequency";
	for (b in freq) print b "," freq[b];
}' dataset_consistent.csv > out/track_name_length_buckets.txt

echo "saved track_name_length_buckets.txt to out/"

# 6) Signal discovery tailored to feature types
echo "Step 6"
awk -F, 'NR>1 {
    genre = tolower($21);
    gsub(/"/,"",genre);
    album = tolower($4);
    gsub(/"/,"",album);
    key = genre SUBSEP album;
    if (!(key in seen)) {
        seen[key]++;
        n = split(album, words, /[^a-z0-9]+/);
        for (i=1; i<=n; i++) {
            w = words[i];
            if (length(w)>1 && w !~ /^(the|a|an|and|or|in|of|on|for|with|ao|de)$/) {
                freq[genre","w]++;
            }
        }
    }
}
END {
    print "genre,word,count";
    for (k in freq) print k "," freq[k];
}' dataset_consistent.csv | sort -t, -k3,3nr > out/album_word_by_genre.txt

echo "saved album_word_by_genre.txt to out/"
