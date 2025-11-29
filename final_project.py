from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

import os
import numpy as np
import matplotlib.pyplot as plt

import subprocess

def parse_args():
    if len (os.sys.argv) >= 2:
        bucket= os.sys.argv[1]
    input_path = f'gs://{bucket}/dataset.csv'
    output_path = f'gs://{bucket}/output'
    return bucket, input_path, output_path


# GCS_BUCKET = "131-final-cp"
# INPUT_PATH = "gs://131-final-cp/dataset.csv"
# OUTPUT_PATH = "gs://131-final-cp/output"

GCS_BUCKET, INPUT_PATH, OUTPUT_PATH = parse_args()

NUM_REPARTITIONS = 64 
NUM_COALESCE = 4

FEATURE_COLS = [
    "danceability",
    "energy",
    "valence",
    "tempo",
    "loudness",
    "acousticness",
    "instrumentalness",
    "liveness",
    "speechiness"
]


def create_spark():
    spark = (
        SparkSession.builder
        .appName("FinalProject")
        .master("local[*]")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hconf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    return spark

def read_data(spark):
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(INPUT_PATH))
    print("Initial partitions:", df.rdd.getNumPartitions())
    df = df.repartition(NUM_REPARTITIONS, "track_id")
    print("After initial repartition:", df.rdd.getNumPartitions())
    return df

def clean_data(df):
    df = df.dropna(subset=["track_id", "track_name", "artists"])
    df = df.withColumn("artists", F.trim("artists"))
    df = df.withColumn("track_name", F.trim("track_name"))
    # If duration_ms exists, convert to minutes
    if "duration_ms" in df.columns:
        df = df.withColumn("duration_min", F.col("duration_ms") / 60000.0)
    df = df.filter(F.length("artists") > 0)
    print("After cleaning partitions:", df.rdd.getNumPartitions())
    return df

def explode_artists(df):
    df = df.repartition(NUM_REPARTITIONS, "artists")
    print("Before explode partitions:", df.rdd.getNumPartitions())
    exploded = (df.withColumn("artist", F.explode(F.split(F.col("artists"), ";")))
                  .withColumn("artist", F.trim("artist")))
    print("After explode partitions:", exploded.rdd.getNumPartitions())
    return exploded

def per_artist_stats(df):
    df = df.repartition(NUM_REPARTITIONS, "artist")
    stats = (df.groupBy("artist")
               .agg(
                   F.count("*").alias("num_tracks"),
                   F.avg("popularity").alias("avg_popularity"),
                   F.avg("danceability").alias("avg_danceability"),
                   F.avg("energy").alias("avg_energy"),
                   F.avg("duration_min").alias("avg_duration_min"),
               )
               .orderBy(F.desc("num_tracks")))
    print("Partitions after groupBy:", stats.rdd.getNumPartitions())
    return stats

def write_results(per_artist):
    print("Partitions before coalesce:", per_artist.rdd.getNumPartitions())
    per_artist_small = per_artist.coalesce(NUM_COALESCE)
    print("Partitions after coalesce:", per_artist_small.rdd.getNumPartitions())
    per_artist_small.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/per_artist")



def segmented_kpis_by_popularity(df):

    # make sure everything is double not a string
    df = df.withColumn("popularity", F.col("popularity").cast("double"))
    for c in FEATURE_COLS:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))

    # make buckets for popularity
    bucket = (
    F.when(F.col("popularity") < 20, "00-19")
        .when(F.col("popularity") < 40, "20-39")
        .when(F.col("popularity") < 60, "40-59")
        .when(F.col("popularity") < 80, "60-79")
        .otherwise("80-100")
    )

    # add popularity bucket column
    df = df.withColumn("popularity_bucket", bucket)

    # count num of tracks in per bucket
    aggs = [F.count("*").alias("num_tracks")]
    for c in FEATURE_COLS:
        aggs.append(F.avg(c).alias(f"avg_{c}"))

    # group by bucket
    res = (
        df.groupBy("popularity_bucket")
            .agg(*aggs)
            .orderBy("popularity_bucket")
        )

    return res

def write_kpis(seg_kpis):
    # a) unpartitioned
    print("KPI (unpartitioned) partitions before coalesce:", seg_kpis.rdd.getNumPartitions())
    seg_kpis_unpart = seg_kpis.coalesce(NUM_COALESCE)
    seg_kpis_unpart.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/segmented_kpis_unpartitioned")

    # b) partitioned by bucket (will produce sub folders of diff buckets)
    seg_kpis_part = seg_kpis.coalesce(NUM_COALESCE)
    (seg_kpis_part
        .write
        .mode("overwrite")
        .partitionBy("popularity_bucket")
        .parquet(f"{OUTPUT_PATH}/segmented_kpis_partitioned"))

def popularity_correlations(df, feature_cols):
    '''
    compute pearson correlation between popularity and each feature col
    writes a small table to OUTPUT_PATH/feature_correlations.
    '''
    # Keep only needed feature columns and drop nulls
    cols_to_keep = ['popularity']
    for c in feature_cols:
        if c in df.columns:
            cols_to_keep.append(c)
    df_num = df.select(cols_to_keep).dropna(subset=["popularity"])

    # partition just for numeric analysis
    df_num = df_num.repartition(NUM_REPARTITIONS)

    rows = []
    for col in feature_cols:
        if col not in df_num.columns:
            print(f"[correlations] Skipping missing column: {col}")
            continue
        # compute pearson correlation
        corr = df_num.stat.corr(col, "popularity")
        rows.append((col, float(corr)))

    spark = df_num.sparkSession
    corr_df = spark.createDataFrame(rows, ["feature", "pearson_corr"])
    corr_df = corr_df.orderBy(F.desc(F.abs("pearson_corr")))

    corr_df.show(truncate=False)

    # Write result table
    corr_df.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/feature_correlations")

    return corr_df


def popularity_regressions(df, feature_cols):
    '''
    Linear regression line fitting popularity ~ feature_col for each feature col
    '''
    cols_to_keep = ['popularity']
    for c in feature_cols:
        if c in df.columns:
            cols_to_keep.append(c)
    df_num = df.select(cols_to_keep).dropna(subset=["popularity"])

    df_num = df_num.repartition(NUM_REPARTITIONS)

    spark = df_num.sparkSession
    results = []

    for col in feature_cols:
        if col not in df_num.columns:
            print(f"[regression] Skipping missing column: {col}")
            continue

        # prepare data for MLlib by converting feature col into a vector
        assembler = VectorAssembler(inputCols=[col], outputCol="features")
        feat_df = assembler.transform(df_num.select(col, "popularity")).select(
            "features", "popularity"
        )

        # fit linear regression model
        lr = LinearRegression(featuresCol="features", labelCol="popularity")
        model = lr.fit(feat_df)

        # slope / coefficient
        slope = float(model.coefficients[0])
        
        intercept = float(model.intercept)
        # variance
        r2 = float(model.summary.r2)

        results.append((col, slope, intercept, r2))

    reg_df = spark.createDataFrame(results, ["feature", "slope", "intercept", "r2"])
    reg_df = reg_df.orderBy(F.desc("r2"))

    reg_df.show(truncate=False)

    reg_df.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/feature_regressions")

    return reg_df

def scatter_with_reg_line(df, feature, sample_frac=0.05, seed=42, output_dir="plots"):
    '''
    create a scatter plot of feature vs popularity with a regression line
    saves a png file to output directory
    '''
    # sample 5% df for plotting so its not too many points (would be unneccessarily slow)
    sdf = (
        df.select(feature, "popularity")
          .dropna()
          .sample(withReplacement=False, fraction=sample_frac, seed=seed)
    )

    # convert to pandas for plotting, easier for matplotlib later
    pdf = sdf.toPandas()

    # axises of plot
    x = pdf[feature].values
    y = pdf["popularity"].values

    # fit regression line y = m*x + b
    m, b = np.polyfit(x, y, 1)

    # make sure output dir exists
    os.makedirs(output_dir, exist_ok=True)

    # create figure
    plt.figure(figsize=(6, 4))

    # plot points
    plt.scatter(x, y, alpha=0.3, s=10)

    x_line = np.linspace(x.min(), x.max(), 100)
    y_line = m * x_line + b

    # plot regression line on the scatter
    plt.plot(x_line, y_line)

    plt.xlabel(feature.capitalize())
    plt.ylabel("Popularity")
    plt.title(f"{feature.capitalize()} vs Popularity")

    out_path = os.path.join(output_dir, f"{feature}_vs_popularity.png")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()

    print(f"[plot] Saved {out_path} (slope={m:.3f}, intercept={b:.3f})")


def generate_plots(df, features, output_dir="plots"):
    '''
    Generate scatter+regression plots for a list of features.
    '''
    for col in features:
        if col in df.columns:
            scatter_with_reg_line(df, col, output_dir=output_dir)
        else:
            print(f"[plot] Skipping missing column: {col}")

# was working with cloud vm so use gsutil to copy plots to bucket
def plot_to_bucket():
    subprocess.run(f"gsutil cp plots/*.png {OUTPUT_PATH}/plots/", shell = True)


def main():
    spark = create_spark()
    df = read_data(spark)
    df = clean_data(df)

    if "track_id" in df.columns:
        df_tracks = df.dropDuplicates(["track_id"])  # prevent double-counting
    else:
        df_tracks = df

    seg_kpis = segmented_kpis_by_popularity(df_tracks)
    write_kpis(seg_kpis)  # writes both unpartitioned & partitioned outputs
    exploded = explode_artists(df)
    per_artist = per_artist_stats(exploded)
    write_results(per_artist)

    popularity_correlations(df, FEATURE_COLS)
    popularity_regressions(df, FEATURE_COLS)


    FEATURES_TO_PLOT = FEATURE_COLS
    generate_plots(df, FEATURES_TO_PLOT, output_dir="plots")

    spark.stop()

    plot_to_bucket()

if __name__ == "__main__":
    main()
