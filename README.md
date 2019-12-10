# Data Analyzer 

### What architecture do you suggest that will support those requirements?

 - EMR/Data Proc Spark cluster that created by scheduler and keep results on S3

### How will you schedule this computation?

 - Will use some flow management software: AirFlow, chrontab, etc.

### How will you do the actual computation?

  1. Will create yearly data frame if not exist, which would conrain date that taken from file path of the input. 
  2. If previous yearly aggregation exists, will use it by adding today data and reducing data -366 days, some sort of windowing on yearly.
  3. Will run anlytics on vierw that is based no yearly data, with filters about appropriate dates.

### Where will you keep the results?

  - Distributed storage - S3 or some distributed DB, depends on requirements.

### How will you make sure the system can handle up to 1 year of data in a timely manner?
 
 - We need to make sure that memory propvisioning is big enough to contain yearly data

### How is the Activity and Module Aggregation calculated?

 SELECT count(DISTINCT(module)),user_id, account_id FROM analytics where to_date < $to and to_date > $from group by user_id, account_id

### Write a spark program that shows that.
  - done

### How is the number of unique users calculated?


"SELECT COUNT(DISTINCT(user_id)),user_id, account_id FROM analytics where to_date < $to and to_date > $from group by account_id, user_id

SELECT count(DISTINCT(module)),user_id, account_id FROM analytics where to_date < $to and to_date > $from group by user_id, account_id

