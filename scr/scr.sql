CREATE TEMPORARY TABLE IF NOT EXISTS customers_temp (
    index INT, customer_id STRING, first_name STRING, last_name STRING,
    company STRING, city STRING, country STRING, phone_1 STRING,
    phone_2 STRING, email STRING, subscription_date DATE, website STRING,
    subscription_year INT, group INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/hadoop/data/customers.csv' OVERWRITE INTO TABLE customers_temp;

set hive.enforce.bucketing=true;

CREATE TABLE IF NOT EXISTS customers (
    index INT, customer_id STRING, first_name STRING, last_name STRING,
    company STRING, city STRING, country STRING, phone_1 STRING,
    phone_2 STRING, email STRING, subscription_date DATE, website STRING,
    group INT
)
PARTITIONED BY (subscription_year INT)
CLUSTERED BY (first_name, last_name, email) INTO 10 BUCKETS
STORED AS PARQUET;

INSERT INTO TABLE customers PARTITION(subscription_year=2020)
SELECT index, customer_id, first_name, last_name,
       company, city, country, phone_1, phone_2,
       email, subscription_date, website, group
FROM customers_temp WHERE subscription_year=2020;

INSERT INTO TABLE customers PARTITION(subscription_year=2021)
SELECT index, customer_id, first_name, last_name,
       company, city, country, phone_1, phone_2,
       email, subscription_date, website, group
FROM customers_temp WHERE subscription_year=2021;

INSERT INTO TABLE customers PARTITION(subscription_year=2022)
SELECT index, customer_id, first_name, last_name,
       company, city, country, phone_1, phone_2,
       email, subscription_date, website, group
FROM customers_temp WHERE subscription_year=2022;

---------------------------------------------------------------------

CREATE TEMPORARY TABLE IF NOT EXISTS organizations_temp (
    index INT, organization_id STRING, name STRING, website STRING,
    country STRING, description STRING, founded INT, industry STRING,
    number_of_employees INT, group INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/hadoop/data/organizations.csv' OVERWRITE INTO TABLE organizations_temp;

CREATE TABLE IF NOT EXISTS organizations (
    index INT, organization_id STRING, name STRING, website STRING,
    country STRING, description STRING, founded INT, industry STRING,
    number_of_employees INT, group INT
)
CLUSTERED BY(name) INTO 10 BUCKETS
STORED AS PARQUET;

INSERT INTO TABLE organizations
SELECT *
FROM organizations_temp;

---------------------------------------------------------------------

CREATE TEMPORARY TABLE IF NOT EXISTS people_temp (
    index INT, user_id STRING, first_name STRING, last_name STRING,
    sex STRING, email STRING, phone STRING, date_of_birth DATE,
    job_title STRING, group INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/hadoop/data/people.csv' OVERWRITE INTO TABLE people_temp;

CREATE TABLE IF NOT EXISTS people (
    index INT, user_id STRING, first_name STRING, last_name STRING,
    sex STRING, email STRING, phone STRING, date_of_birth DATE,
    job_title STRING, group INT
)
CLUSTERED BY(first_name, last_name, email) INTO 10 BUCKETS
STORED AS PARQUET;

INSERT INTO TABLE people
SELECT *
FROM people_temp;

---------------------------------------------------------------------

CREATE TABLE summary_statistics AS

WITH cust_union AS (
    SELECT first_name, last_name, email, customer_id, company, subscription_date, subscription_year
    FROM customers
    WHERE subscription_year = 2020
    UNION
    SELECT first_name, last_name, email, customer_id, company, subscription_date, subscription_year
    FROM customers
    WHERE subscription_year = 2021
    UNION
    SELECT first_name, last_name, email, customer_id, company, subscription_date, subscription_year
    FROM customers
    WHERE subscription_year = 2022
),
customers_count_with_age_group AS (
    SELECT cu.company, cu.subscription_year, COUNT(*) cs,
        CASE
            WHEN YEAR(CURRENT_DATE) - YEAR(p.date_of_birth) <= 18 THEN '0 - 18'
            WHEN YEAR(CURRENT_DATE) - YEAR(p.date_of_birth) <= 25 THEN '19 - 25'
            WHEN YEAR(CURRENT_DATE) - YEAR(p.date_of_birth) <= 35 THEN '26 - 35'
            WHEN YEAR(CURRENT_DATE) - YEAR(p.date_of_birth) <= 45 THEN '36 - 45'
            WHEN YEAR(CURRENT_DATE) - YEAR(p.date_of_birth) <= 55 THEN '46 - 55'
            WHEN YEAR(CURRENT_DATE) - YEAR(p.date_of_birth) <= 75 THEN '56 - 75'
            WHEN YEAR(CURRENT_DATE) - YEAR(p.date_of_birth) <= 100 THEN '76 - 100'
            ELSE '101 - ∞'
        END AS age_group
    FROM cust_union cu
    JOIN people p ON cu.first_name = p.first_name AND cu.last_name = p.last_name AND cu.email = p.email
    JOIN organizations org ON cu.company = org.name
    GROUP BY cu.company, cu.subscription_year, p.date_of_birth
)

SELECT company, subscription_year, age_group, MAX(cs)
FROM customers_count_with_age_group
GROUP BY company, subscription_year, age_group;
