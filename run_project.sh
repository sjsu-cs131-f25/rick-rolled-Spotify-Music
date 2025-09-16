mkdir -p out

grep -vi '".*,.*"' dataset.csv > dataset_clean.csv

tail -n +2 dataset_clean.csv | cut -d ',' -f9 | sort | uniq -c | sort -nr > out/freq_danceability.txt

tail -n +2 dataset_clean.csv | cut -d ',' -f10 | sort | uniq -c | sort -nr > out/freq_energy.txt 2> out/errors.txt

tail -n +2 dataset_clean.csv | cut -d ',' -f21 | sort | uniq -c | sort -nr | head -n 50 | tee -a out/music_top_50_genres.txt
