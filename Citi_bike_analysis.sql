-- Databricks notebook source
-- No of rows in the Dataset
select count (*) from citibike_data;

-- COMMAND ----------

--Viewing Dataset
select * from citibike_data limit 100;

-- COMMAND ----------

--How many citibikes are there in NYC?
select count(distinct(bikeid)) as number_of_bikes
from citibike_data;

-- COMMAND ----------

 --let's determine the top 20 most popular stations where customers start their trip?
select  `start station name`,
        `start station latitude`,
        `start station longitude`,
        count (*) as number_of_trips
from    citibike_data
group by
        `start station name`,
        `start station latitude`,
        `start station longitude`
order by
        number_of_trips desc
limit 20;

-- COMMAND ----------

--what is the difference in average in trip-duration for subscribers vs. non-subscribers?
select  usertype,
        count(*) as num_of_trips, 
        round(avg(tripduration / 60), 2) as duration
from    citibike_data
group by
        usertype; 

-- COMMAND ----------

--These are the top routes (trips) broken down by subscribers
select  usertype,  
        concat(`start station name`, " to ", `end station name`) as route,  
        count(*) as number_of_trips,  
        round(avg(tripduration / 60),2) as duration
from    citibike_data
where   usertype in ('Subscriber')
group by  
      `start station name`, 
      `end station name`, 
       usertype
order by  
      number_of_trips desc
limit 20;

-- COMMAND ----------

--These are the top routes (trips) broken down by subscribers and customers
select  usertype,  
        concat(`start station name`, " to ", `end station name`) as route,  
        count(*) as number_of_trips,  
        round(avg(tripduration / 60),2) as duration
from    citibike_data
where   usertype in ('Customer')
group by  
      `start station name`, 
      `end station name`, 
       usertype
order by  
      number_of_trips desc
limit 20;

-- COMMAND ----------

-- The following query returns the number of trips by age and gender for 2017 (limited to less than 90 years old)
select  (2017 - cast(`birth year` as int)) as age,
        count(*) total_count, 
        count(case when gender = 1 then 1 end) as number_of_males,
        count(case when gender = 2 then 1 end) as number_of_females, 
        count(case when gender = 0 then 1 end) as number_of_unknown
from    citibike_data
where   cast(`birth year` as string) != 'NULL' and
        (2017 - cast(`birth year` as int)) <= 90
group by age
order by age;

-- COMMAND ----------

--Let's explore the bike routes (trips) by gender, to see if they are similar.
select  concat(`start station name`, " to ", `end station name`) as route,  
        count(*) as number_of_trips 
from    citibike_data
where   gender = 2 --female
group by 
        `start station name`, `end station name`
order by number_of_trips desc
limit 5;

-- COMMAND ----------

select  concat(`start station name`, " to ", `end station name`) as route,  
        count(*) as number_of_trips 
from    citibike_data
where   gender = 1 --male
group by 
        `start station name`, `end station name`
order by number_of_trips desc
limit 5;

-- COMMAND ----------

--Every citibike has a unique bike ID (bikeid column), which bike had the most trips in 2017?
select  bikeid,  
        count(*) as trip_count 
from    citibike_data
group by bikeid
order by trip_count desc
limit 5;

-- COMMAND ----------


