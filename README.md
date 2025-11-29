# Spotify Tracks Dataset Analysis
A dataset of Spotify songs with different genres and their audio features. <br/>
### Rick-Rolled (Team 7)
Aaron Pang, Ethan Tran, Chloe Pham, Samuel Leonetti, Dan Nguyen

## Data Card

Source: Spotify Tracks Dataset (Kaggle)  
File: `dataset_clean.csv` (~20 MB)  
Format: CSV (comma-separated), UTF-8 encoding    

Columns:
- track_id  
- artists  
- album_name  
- track_name  
- popularity  
- duration_ms  
- explicit  
- danceability  
- energy  
- key  
- loudness  
- mode  
- speechiness  
- acousticness  
- instrumentalness    
- tempo    
- track_genre  

Size:
- Rows: ~106350 (from `wc -l`)  
- Columns: 21 (from `awk -F',' '{print NF}'`)  

Samples:
- A 1,000-row random sample (plus header) saved at:  
  `data/samples/sample_1k.csv`  
### How to Run final_project.py
- ensure that dataset.csv exists in the same exact directory as final_project.py
- run in VM and then run the following command using your bucket name as a Python argument
- time spark-submit --conf spark.ui.enabled=true --conf spark.ui.port=4040 --master local[*] ~/final_project.py <BUCKET NAME>

