# quick_check.py 
import sqlite3
import pandas as pd

with sqlite3.connect("data/warehouse.db") as conn:
    print(pd.read_sql_query("SELECT * FROM daily_visit_metrics ORDER BY visit_date;", conn))
