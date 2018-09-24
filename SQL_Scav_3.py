# Your code goes here :)
import bq_helper
import matplotlib.pyplot as plt

accidents = bq_helper.BigQueryHelper(active_project='bigquery-public-data',dataset_name = "nhtsa_traffic_fatalities")

query1 = """ SELECT COUNT(consecutive_number),
                   EXTRACT(HOUR FROM timestamp_of_crash)
            FROM `bigquery-public-data.nhtsa_traffic_fatalities.accident_2015`
            GROUP BY EXTRACT(HOUR FROM  timestamp_of_crash)
            ORDER BY COUNT(consecutive_number) DESC
        """
#Accidents per Hour of Day
accidents2015_per_HoD = accidents.query_to_pandas_safe(query1)
print(accidents2015_per_HoD)
plt.bar(accidents2015_per_HoD.f1_,accidents2015_per_HoD.f0_)

query2 = """ SELECT COUNT(consecutive_number),
                   EXTRACT(HOUR FROM timestamp_of_crash)
            FROM `bigquery-public-data.nhtsa_traffic_fatalities.accident_2016`
            GROUP BY EXTRACT(HOUR FROM  timestamp_of_crash)
            ORDER BY COUNT(consecutive_number) DESC
        """

accidents2016_per_HoD = accidents.query_to_pandas_safe(query2)
print(accidents2016_per_HoD)
plt.bar(accidents2016_per_HoD.f1_,accidents2016_per_HoD.f0_)

query3 = """SELECT registration_state_name, COUNT(hit_and_run)
            FROM `bigquery-public-data.nhtsa_traffic_fatalities.vehicle_2015`
            WHERE hit_and_run = 'Yes'
            GROUP BY registration_state_name
            ORDER BY COUNT(hit_and_run) DESC
         """

asdf = accidents.query_to_pandas_safe(query3)
print(asdf)
#find a better way to display this data
plt.bar(asdf.registration_state_name,asdf.f0_)

query4 =""" SELECT registration_state_name,
                   COUNT(hit_and_run)
            FROM `bigquery-public-data.nhtsa_traffic_fatalities.vehicle_2016`
            WHERE hit_and_run = 'Yes'
            GROUP BY registration_state_name
            ORDER BY COUNT(hit_and_run) DESC
        """

jkl = accidents.query_to_pandas_safe(query4)
print(jkl)
plt.bar(jkl.registration_state_name,jkl.f0_)