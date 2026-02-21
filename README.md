# home_who
ETL Pipeline Project

# Target 
1. extracts data from the WHO GHO OData API,

2. transforms the data in any reasonable way,

3. loads it into a PostgreSQL database.

# Setup and Db configuration 

service uri: postgres://avnadmin:password@pg-29eb9bc4-byja1101-who.k.aivencloud.com:28387/defaultdb?sslmode=require 

db_name = defaultdb 

host = pg-29eb9bc4-byja1101-who.k.aivencloud.com

port = 28387 

user = avnadmin 

password = ask for one 

ssl mode : require 

connection limit : 20 

## 1. Prerequisites
- Python 3.11+

## 3. Install Python deps
```bash
pip install -r requirements.txt
```

## 4. Run the pipeline
```bash
# Full run
python etl.py