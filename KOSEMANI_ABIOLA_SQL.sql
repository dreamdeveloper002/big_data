-- Databricks notebook source
drop table if exists clinicaltrial_2021;

create table clinicaltrial_2021 
using csv
options(
header='true',
delimiter='|',
inferSchema='true',
mode='FAILFAST',
path='/FileStore/tables/clinicaltrial_2019.csv'
);

cache table clinicaltrial_2021

-- COMMAND ----------


drop table if exists pharma;

create table pharma
using csv
options(
header='true',
delimiter=',',
inferSchema='true',
mode='PERMISSIVE',
path='/FileStore/tables/pharma.csv'
);

cache table pharma

-- COMMAND ----------

select * from clinicaltrial_2021

-- COMMAND ----------

select * from pharma

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### QUESTION 1

-- COMMAND ----------

select count(*) from clinicaltrial_2021;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### QUESTION 2

-- COMMAND ----------

SELECT Type,
COUNT(Type) AS frequency
FROM clinicaltrial_2021
GROUP BY Type
HAVING Type IS NOT NULL
ORDER BY frequency DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### QUESTION 3

-- COMMAND ----------

WITH ct AS (
  SELECT explode(split(Conditions, ',')) AS separate_condition
  FROM clinicaltrial_2021
)
SELECT separate_condition AS condition, COUNT(*) AS Count
FROM ct
GROUP BY separate_condition
ORDER BY Count DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### QUESTION 4

-- COMMAND ----------

---create a view to show the sponsors
 
create view if not exists SPONSORS as
select Sponsor
from clinicaltrial_2021;
 
select * from SPONSORS

-- COMMAND ----------

---create a view for the pharmaceutical companies
 
create view if not exists pharmaceutical as
select Parent_Company
from pharma;
 
select * from pharmaceutical 


-- COMMAND ----------

---create a view for sponsors that are not pharmaceutical companies
 
create view if not exists Sponsors_not_pharmaceuticals as
select * from SPONSORS
where Sponsor not in (select Parent_Company from pharmaceutical);
 
 
select * from Sponsors_not_pharmaceuticals

-- COMMAND ----------

---10 most common sponsors that are not pharmaceuticals
 
select Sponsor, count(*) as Count
from Sponsors_not_pharmaceuticals
where Sponsor is not null
group by Sponsor
order by Count desc
limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### QUESTION 5

-- COMMAND ----------

select left(Completion, 3) as Months, count(*) as No_Of_Completed_Studies
from clinicaltrial_2021
where Status = 'Completed' and Completion !='' and right(Completion, 4) = '2021'
group by Months
order by No_of_Completed_Studies desc;
