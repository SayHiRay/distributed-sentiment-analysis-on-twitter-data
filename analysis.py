
# coding: utf-8

# In[40]:


from pyspark.ml.feature import RegexTokenizer, HashingTF, IDF, CountVectorizer, Normalizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField
from pyspark.sql import SparkSession
from pyspark.ml.feature import StopWordsRemover
from nltk.sentiment import SentimentIntensityAnalyzer


# # Data Retrieval

# In[41]:


data_file = r"C:/Users/Robert/PycharmProjects/distributed-sentiment-analysis-on-twitter-data/twitter_scraper/twitter_data_final.csv"
vader_analyzer = SentimentIntensityAnalyzer()


# In[42]:


# Initialize a Spark session
spark = SparkSession     .builder     .appName("SentimentAnalysis")     .getOrCreate()


# In[43]:


# define the data schema(format/structure) for our twitter data in the csv file
twitter_data_schema = StructType([StructField("date_str", StringType(), True),
                                  StructField("tweet_id", StringType(), True),
                                  StructField("text", StringType(), True),
                                  StructField("location", StringType(), True),
                                  StructField("user_id", StringType(), True),
                                  StructField("user_name", StringType(), True),
                                  StructField("user_location", StringType(), True),
                                  StructField("user_url", StringType(), True),
                                  StructField("user_description", StringType(), True),
                                  StructField("place_id", StringType(), True),
                                  StructField("place_url", StringType(), True),
                                  StructField("place_type", StringType(), True),
                                  StructField("place_countrycode", StringType(), True),
                                  StructField("place_country", StringType(), True),
                                  StructField("place_boundingboxtype", StringType(), True),
                                  StructField("entities_hashtags", StringType(), True),
                                  StructField("entities_urls", StringType(), True),
                                  StructField("entities_mentions", StringType(), True),
                                  StructField("entities_symbols", StringType(), True),
                                  StructField("entities_media", StringType(), True),
                                  StructField("entities_polls", StringType(), True),])


# In[44]:


df_raw = spark.read.csv(
    data_file, schema=twitter_data_schema
)


# In[45]:


df_raw.show(truncate=False)


# # Tweet Cleaning Function

# In[46]:


import re
from bs4 import BeautifulSoup
from nltk.tokenize import WordPunctTokenizer
tok = WordPunctTokenizer()

at_user_pat = r'@[A-Za-z0-9_]+'  # r'@[\w]+'
url_pat = r'https?://[^ ]+'  # r'https?:\/\/[^\s]+'
www_pat = r'www.[^ ]+'
repeating_chars_pat = r'([A-Za-z])\1+'
negations_dic = {"isn't":"is not", "aren't":"are not", "wasn't":"was not", "weren't":"were not",
                "haven't":"have not","hasn't":"has not","hadn't":"had not","won't":"will not",
                "wouldn't":"would not", "don't":"do not", "doesn't":"does not","didn't":"did not",
                "can't":"can not","couldn't":"could not","shouldn't":"should not","mightn't":"might not",
                "mustn't":"must not"}
neg_pattern = re.compile(r'\b(' + '|'.join(negations_dic.keys()) + r')\b')

def tweet_cleaner(text):
    soup = BeautifulSoup(text, 'lxml')
    souped = soup.get_text()
    try:
        bom_removed = souped.decode("utf-8-sig").replace(u"\ufffd", "?")
    except:
        bom_removed = souped
    stripped = re.sub(at_user_pat, 'USERNAME', bom_removed)
    stripped = re.sub(url_pat, 'URL', stripped)
    stripped = re.sub(www_pat, 'URL', stripped)
    stripped = re.sub(repeating_chars_pat, r'\1\1', stripped)

    lower_case = stripped.lower()
    neg_handled = neg_pattern.sub(lambda x: negations_dic[x.group()], lower_case)
    letters_only = re.sub("[^a-zA-Z]", " ", neg_handled)
    # During the letters_only process two lines above, it has created unnecessay white spaces,
    # I will tokenize and join together to remove unneccessary white spaces
    words = [x for x in tok.tokenize(letters_only) if len(x) > 1]
    return (" ".join(words)).strip()


# # Data Pre-Processing

# In[47]:


df_filtered = df_raw.filter("location like '%,___'")
df_filtered.show(truncate=True)


# In[48]:


udf_tweet_cleaner = udf(tweet_cleaner)
df_preprocessed = df_filtered.withColumn("text", udf_tweet_cleaner(col("text")))
df_preprocessed.show(truncate=True)


# # Sentiment Analysis

# In[49]:


def sentiment_analysis(text):
    return 1

udf_sentiment_analysis = udf(sentiment_analysis)


# In[50]:


df_scored = df_preprocessed.withColumn("score", udf_sentiment_analysis(col("text")))


# In[51]:


df_scored.select('text', 'score').show(n=100,truncate=False)


# # Aggregation and Sorting

# In[52]:


df_scored.groupBy('location').agg({'score': 'sum'}).sort('sum(score)', ascending=False).show(n=100000,truncate=False)


# # Stop Spark

# In[53]:


spark.stop()

