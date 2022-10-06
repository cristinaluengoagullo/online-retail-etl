# online-retail-etl

ETL for the annotation of online retail transactions. The information is integrated from an API which determines whether a date was a national holiday (given a date and a country).

* Dataset: https://archive.ics.uci.edu/ml/datasets/Online+Retail#
* REST API: https://getfestivo.com/

Once integrated, the data is loaded into a Snowflake database.

**API restrictions**

* The basic plan includes 1000 requests
* Only data from the previous year can be queried 

