# Your code goes here :)
import bq_helper

hacker_news = bq_helper.BigQueryHelper(active_project = "bigquery-public-data", dataset_name = "hacker_news")

hacker_news.head("full", 10)

query = """SELECT type, COUNT(ID)
            FROM `bigquery-public-data.hacker_news.full`
            GROUP BY type"""

types_of_comments = hacker_news.query_to_pandas_safe(query)

print(types_of_comments)

query2 = """SELECT deleted, COUNT(ID)
            FROM `bigquery-public-data.hacker_news.full`
            GROUP BY deleted"""
#num of comments deleted
ncd = hacker_news.query_to_pandas_safe(query2)

print(ncd)