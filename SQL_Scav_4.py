import bq_helper
import matplotlib.pyplot as plt

bitcoin_blockchain = bq_helper.BigQueryHelper(active_project = "bigquery-public-data", dataset_name="bitcoin_blockchain")
query1 ="""WITH time AS
            (
                SELECT TIMESTAMP_MILLIS(timestamp) AS trans_time,
                    transaction_id
                FROM `bigquery-public-data.bitcoin_blockchain.transactions`
            )
            
            SELECT COUNT(transaction_id) AS unique_transid_num, 
                    EXTRACT(DAY FROM trans_time) AS DAY
            FROM time
            GROUP BY DAY
            ORDER BY unique_transid_num DESC        
        """

transactions_per_day = bitcoin_blockchain.query_to_pandas_safe(query1, max_gb_scanned=21)

print(transactions_per_day)

plt.bar(transactions_per_day.DAY,transactions_per_day.unique_transid_num)

query2 ="""SELECT merkle_root,
                COUNT(transaction_id) AS trans_count
        FROM `bigquery-public-data.bitcoin_blockchain.transactions`
         GROUP BY merkle_root
        """
    
transaction_per_merkle = bitcoin_blockchain.query_to_pandas_safe(query2,max_gb_scanned=38)

print(transaction_per_merkle)