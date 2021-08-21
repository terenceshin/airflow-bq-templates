# STEP 1: Execute query in BQ

query = '''
CREATE TABLE dataset.table_name 
(
    status_date DATE
  , event_time TIMESTAMP
  , user STRING
)
PARTITION BY status_date
AS
SELECT ...
FROM ...
WHERE ...
)
'''