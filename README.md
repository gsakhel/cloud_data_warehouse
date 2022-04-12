# Summary
This program will extract music event and song data from S3, transform it into fact and dimension tables, and load it into a newly created Redshift database.

# Instructions
1. requirements.txt -- use to install python packages: pip install -r requirements.txt
1. jsonpaths.json   -- Describes structure of song_table data. Upload to an S3 bucket.
1. dwh.cfg          -- Configuration file. Enter AWS permissions and values here.
1. boot.py          -- Create Redshift cluster, run create_tables.py and etl.py to create data warehouse.
1. test.py          -- Run analytic_queries to test data warehouse.
1. shutdown.py      -- Shutdown Redshift and delete User.


*  create_tables.py  -- Drops tables then creates staging as well as fact and dimension tables.
*  etl.py            -- Extract from S3 and load new database.
*  sql_queries.py    -- SQL used for creating tables (create_tables.py) and inserting data into them (etl.py).
*  etl.ipynb         -- Use to develop etl.py.
*  etl_insert_tables -- Used to troubleshoot fact and dimension tables.


# Useful Links
1. https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html