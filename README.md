# Sentiment Analysis on Twitter Data

This repository is a Master course project for CS4225 at NUS. 

In this project, we build up Spark applications to evaluate the happiness of U.S. cities by performing sentiment analysis on Twitter data. 
We first collect a dataset containing 6.1 millions English tweets tweeted in the U.S. and their geolocation information. 
We then preprocess this scraped dataset and the Sentiment140 dataset, and extract tf-idf features of all tweets. 
4 different models are trained in Spark, and their performance are compared with VADER, 
a popular and effective sentiment analysis model proposed in 2014 and specifically designed for social media data. 
We observe that our LR, LSVC, and NB models achieve good performance comparable to VADER on a human-labeled dataset. 
We measure the happiness of U.S. cities by performing sentiment analysis on tweets of different U.S. cities and aggregating the results. 
All our preprocessing, model training, and happiness measurement are done in Spark, and can easily scale by running the applications on a Spark cluster.

The training is done in `train.ipynb`. 
The analysis is done in `analysis.ipynb`.
The data files are not included due to their large size, but they can be easily re-collected using scripts under `twitter_scraper`.

For more information about this project, please refer to **Report.pdf**.
