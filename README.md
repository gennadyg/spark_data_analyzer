# Data Analyzer 

### What architecture do you suggest that will support those requirements?

 - EMR/Data Proc Spark cluster that created by scheduler and keep results on S3

### How will you schedule this computation?

 - Will use some flow management software: AirFlow, chrontab, etc.

### How will you do the actual computation?

  - Will query data based on required time periods and will aggregate timw window, to avoid duplicated data reads

### Where will you keep the results?

  - Distributed storage - S3 or some distributed DB, depends on requirements.

### How will you make sure the system can handle up to 1 year of data in a timely manner?

### How is the Activity and Module Aggregation calculated?

### Write a spark program that shows that.
  - done

### How is the number of unique users calculated?

SELECT COUNT(DISTINCT(user_id)),user_id, account_id FROM analytics group by account_id, user_id

