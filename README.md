# home_who
ETL Pipeline Project

# Target 
1. extracts data from the WHO GHO OData API,

2. transforms the data in any reasonable way,

3. load it into a PostgreSQL database.

# Configuration and important variable
```bash
DB_DSN = os.getenv(
    "DATABASE_URL", ""
)
API_BASE = "https://ghoapi.azureedge.net/api"
INDICATOR = "WHOSIS_000001"          # Life expectancy at birth
PAGE_SIZE = 1000
CHECKPOINT = "checkpoint.json"        # Stores last processed page for resume
REQUEST_DELAY = 0.3
```

## 1. Prerequisites
pip install -r requirements.txt

## 3. Install Python deps
```bash
pip install -r requirements.txt
```

## 4. Run the pipeline
```bash
# Full run
python etl.py

#DB analysis : 

Global life expectancy grouped by year Trends: 

```sql
SELECT year, ROUND(AVG(value), 1) AS avg_life_expectancy
FROM who_life_expectancy
WHERE sex = 'SEX_BTSX'
GROUP BY year ORDER BY year;
```sql

#Test
pytest test.py -v