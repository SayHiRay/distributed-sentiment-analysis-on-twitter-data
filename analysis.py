from pyspark.ml.feature import RegexTokenizer, HashingTF, IDF, CountVectorizer, Normalizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField
from pyspark.sql import SparkSession
from pyspark.ml.feature import StopWordsRemover
from nltk.sentiment import SentimentIntensityAnalyzer


if __name__ == "__main__":
    # NOTE: for python 2, you may need to run:
    # export PYTHONIOENCODING=utf8
    # in command line first before you run this script.
    # This may prevent some errors related to string encodings.
    data_file = r"file:///root/spark_repos_ruizhi/distributed-sentiment-analysis-on-twitter-data/twitter_scraper/data/twitter_data.csv"

    vader_analyzer = SentimentIntensityAnalyzer()

    # Initialize a Spark session
    spark = SparkSession \
        .builder \
        .appName("SentimentAnalysis") \
        .getOrCreate()

    # define the data schema(format/structure) for our twitter data in the csv file
    twitter_data_schema = StructType([StructField("date_str", StringType(), True),
                                      StructField("text", StringType(), True),
                                      StructField("user_id", StringType(), True),
                                      StructField("location", StringType(), True)])

    df_raw = spark.read.csv(
        data_file, schema=twitter_data_schema
    )

    df_raw.show(truncate=False)

    udf_sentiment_analysis = udf(lambda words: len(words), IntegerType())
    spark.stop()
