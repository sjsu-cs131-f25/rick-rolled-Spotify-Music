#!/usr/bin/awk -f
# remove empty keys and ones with zero popularity
# consistent column count
# positive counts

BEGIN {
  FS = ","
  OFS = "\t"
  expected_cols = 0
}

NR == 1 {
  expected_cols = NF

  for (i = 1; i <= NF; i++) {
    if ($i == "track_id") col_track_id = i
    if ($i == "artists") col_artists = i
    if ($i == "track_name") col_track_name = i
    if ($i == "popularity") col_popularity = i
    if ($i == "duration_ms") col_duration_ms = i
    if ($i == "speechiness") col_speechiness = i
    if ($i == "instrumentalness") col_instrumentalness = i
  }
  next
}

# Quality Filters
{
  # column count consistency
  if (NF != expected_cols) {
    print "SKIP: row " NR " has " NF " cols, expected " expected_cols > "logs/filter_log.txt"
    next
  }

  # non-empty keys
  if ($col_track_id == "" || $col_artists == "" || $col_track_name == "") {
    print "SKIP: row " NR " has empty key (track_id/artists/track_name)" > "logs/filter_log.txt"
    next
  }

  # positive duration count
  if ($col_duration_ms <= 0) {
    print "SKIP: row " NR " has non-positive duration_ms=" $col_duration_ms > "logs/filter_log.txt"
    next
  }

  # non-zero popularity
  if ($col_popularity + 0 == 0) {
    print "SKIP: row " NR " has zero popularity" > "logs/filter_log.txt"
    next
  }

  # output to filtered TSV
  print $0 >> "out/filtered_tracks.tsv"
  passed++

  # per-artist metrics
  pop = $col_popularity + 0
  speechiness = $col_speechiness + 0
  instrumentalness = $col_instrumentalness + 0
  
  # ratio of speechiness to pop
  speech_ratio = speechiness / (pop + 1)

  # bucketize speechiness ratio in HI/MID/LO
  if (speech_ratio >= 0.001) {
    speech_bucket = "HI"
  } else if (speech_ratio >= 0.0005) {
    speech_bucket = "MID"
  } else {
    speech_bucket = "LO"
  }

  # split artist combos
  split($col_artists, artists_array, ";")
  for (i in artists_array) {
    gsub(/^[ \t]+|[ \t]+$/, "", artists_array[i])
    if (artists_array[i] != "") {
      current_artist = artists_array[i]
      artist_count[current_artist]++
      artist_pop[current_artist] += pop
      artist_speechiness[current_artist] += speechiness
      artist_instrumentalness[current_artist] += instrumentalness
      artist_speech_ratio[current_artist] += speech_ratio

      # track bucket per artist
      artist_bucket[current_artist, speech_bucket]++
    }
  }
}

END {
  #per artist summary 
  print "artist\ttrack_count\tavg_popularity\tavg_speechiness\tavg_instrumentalness\tavg_speech_ratio\tbucket_HI\tbucket_MID\tbucket_LO" > "out/per_artist_summary.tsv"

  for (artist in artist_count) {
    count = artist_count[artist]
    avg_pop = artist_pop[artist] / count
    avg_speech = artist_speechiness[artist] / count
    avg_inst = artist_instrumentalness[artist] / count
    avg_ratio = artist_speech_ratio[artist] / count
  
    hi_count = (artist, "HI") in artist_bucket ? artist_bucket[artist, "HI"] : 0
    mid_count = (artist, "MID") in artist_bucket ? artist_bucket[artist, "MID"] : 0
    lo_count = (artist, "LO") in artist_bucket ? artist_bucket[artist, "LO"] : 0
    
    printf "%s\t%d\t%.2f\t%.6f\t%.6f\t%.6f\t%d\t%d\t%d\n", artist, count, avg_pop, avg_speech, avg_inst, avg_ratio, hi_count, mid_count, lo_count >> "out/per_artist_summary.tsv"
  }

  print "Filtered " passed " rows (removed " (NR - 1 - passed) " invalid rows)" >> "logs/filter_log.txt"
}
