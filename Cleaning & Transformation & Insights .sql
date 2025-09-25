use project_nti;
go

if not exists (
    select 1 
    from sys.schemas 
    where name = N'clean'
)
begin
    exec('CREATE SCHEMA clean AUTHORIZATION dbo;');
end
go

------------------------------------------------------------
-- if the table already exists, remove it
if object_id(N'clean.OnlineRetail', 'U') is not null
    drop table clean.OnlineRetail;
go

select  *from[dbo].[Year2010_2011_fixed]

-- create a fresh clean.OnlineRetail table
create table clean.OnlineRetail (
    invoiceno    nvarchar(20)    not null,   -- invoice number
    stockcode    nvarchar(20)    null,       -- product code
    description  nvarchar(4000)  null,       -- product description
    quantity     int             null,       -- number of items
    invoicedate  datetime2(0)    null,       -- date of invoice
    unitprice    decimal(18,4)   null,       -- price per unit
    customerid   int             null,       -- customer ID
    country      nvarchar(100)   null        -- customer country
);
go



insert into clean.OnlineRetail (
    invoiceno, stockcode, description, quantity,
    invoicedate, unitprice, customerid, country
)
select
    isnull(ltrim(rtrim(cast([invoice] as nvarchar(50)))),
           'RND' + cast(abs(checksum(newid())) as varchar(20))
    ) as invoiceno,

    ltrim(rtrim(cast([StockCode] as nvarchar(50)))) as stockcode,
    nullif(ltrim(rtrim(cast([Description] as nvarchar(4000)))), '') as description,

    try_convert(int, [Quantity]) as quantity,
    try_convert(datetime2(0), [InvoiceDate]) as invoicedate,
    try_convert(decimal(18,4), [Price]) as unitprice,
    try_convert(int, [Customer ID]) as customerid,
    nullif(ltrim(rtrim(cast([Country] as nvarchar(100)))), '') as country
from [dbo].[Year2010_2011_fixed]
where [Invoice] is not null
   or [Invoice] is null;   -- include rows with nulls, they’ll get random ID


---row count
select
    sum(case when invoiceno  is null then 1 else 0 end) as null_invoiceno,
    sum(case when stockcode  is null then 1 else 0 end) as null_stockcode,  --1
    sum(case when description is null then 1 else 0 end) as null_description, --1455
    sum(case when quantity   is null then 1 else 0 end) as null_quantity,--1
    sum(case when unitprice  is null then 1 else 0 end) as null_unitprice--1
from clean.OnlineRetail;


select count(*) from[clean].[OnlineRetail]
select count(*) from[dbo].[Year2010_2011_fixed]

--Trimmed text in Invoice/StockCode/Description/Country (remove stray spaces).

--Converted data types with TRY_CONVERT (safer than hard convert).

--Removed exact duplicate rows (avoid double counting).

--Kept only records with Invoice present (others are invalid for analysis).

--Created IsReturn (from invoice prefix) to separate returns vs sales.

--For sales analysis, kept only UnitPrice > 0 and Quantity > 0 (no free/zero or negative lines).

--Built time features (YearMonth, Year, Month) for trend charts.

--Computed TotalPrice (= Quantity × UnitPrice) to measure revenue.



------------------------------------------------------------------------------------------------
-- (cleaning)remove duplicate rows, keep only one copy
;with duplicates as (
    select
        *,
        row_number() over (
            partition by invoiceno, stockcode, description, quantity,
                         invoicedate, unitprice, customerid, country
            order by (select null)
        ) as rn
    from clean.OnlineRetail
)
delete from duplicates
where rn > 1;      --5268
-------------------------------------------------------------
select  [invoiceno]from [clean].[OnlineRetail]
order by [invoiceno]  --536,639
-----------------------------------------------------------------------------------------------
-- (cleaning)deal with rows where InvoiceNo does not match valid rules


select [invoiceno] from[clean].[OnlineRetail] 
where not (
      invoiceno like '[0-9][0-9][0-9][0-9][0-9][0-9]'     -- 6 digits valid
   or invoiceno like 'C[0-9][0-9][0-9][0-9][0-9][0-9]'   -- cancellation valid
);

----------------------
update [clean].[OnlineRetail]
set invoiceno = 'R' + right(abs(checksum(newid())), 6)  -- prefix R + 6 random digits
where not (
      invoiceno like '[0-9][0-9][0-9][0-9][0-9][0-9]'     -- 6 digits valid
   or invoiceno like 'C[0-9][0-9][0-9][0-9][0-9][0-9]'   -- cancellation valid  --(4 rows affected) deleted
);
-----------------------
select [invoiceno] from[clean].[OnlineRetail] 
where not (
      invoiceno like '[0-9][0-9][0-9][0-9][0-9][0-9]'     -- 6 digits valid
   or invoiceno like 'C[0-9][0-9][0-9][0-9][0-9][0-9]'   -- cancellation valid
);

select [invoice] from [dbo].[Year2010_2011_fixed]
where not (
      invoice like '[0-9][0-9][0-9][0-9][0-9][0-9]'     -- 6 digits valid
   or invoice like 'C[0-9][0-9][0-9][0-9][0-9][0-9]'   -- cancellation valid
);
-------------------------(4 rows affected) deleted--------------------------------------------------------

--(cleaning)Update invalid stock codes with random values

select distinct stockcode
from [dbo].[Year2010_2011_fixed]
where stockcode not like '[0-9][0-9][0-9][0-9][0-9]';  --1012 distinct/54874

select  stockcode
from [clean].[OnlineRetail]
where stockcode not like '[0-9][0-9][0-9][0-9][0-9]'; --1012 distinct or 54488 not distinct

select distinct stockcode
from [clean].[OnlineRetail]
where stockcode is null;      --one row

update clean.OnlineRetail
set stockcode = 'P' + right(abs(checksum(newid())), 5)  -- P + 5 random digits
where stockcode not like '[0-9][0-9][0-9][0-9][0-9]' or stockcode is null ;   --54489
 
 -------------------------------------------------------------------------------------------------------------------------

--(mark invalid (rows/column) instead of deleting)
------------------------------------------------------------------------------------------------------------------
--(cleaning)Update in null description codes with unknown values

select [description]
from [clean].[OnlineRetail]
where [description] is null    --1455
 
update [clean].[OnlineRetail]
set [description]='unknown'
where [description] is null   


select [description] from [dbo].[Year2010_2011_fixed]
where [description] is null --1455

select [description]
from [clean].[OnlineRetail]
where [description] is null --0
------------------------------------------------------------------------------------------------------------------
---(cleaning) (is not cancel) [quantity] <0 and not cancel and price =0 mean not affect on revenue and customerid not exist 

select * from [clean].[OnlineRetail]
order by [unitprice]

select * from [dbo].[Year2010_2011_fixed]
where [invoice] not like 'C%' and [quantity] <0   and[customer id] is null and [price]=0 --1336

select * from [clean].[OnlineRetail]
where [invoiceno] not like 'C%' and [quantity] <0   and[customerid] is null and [unitprice]=0    --1336


delete from [clean].[OnlineRetail]
where [invoiceno] not like 'C%' and [quantity] <0   and[customerid] is null and [unitprice]=0  --1336
--------------------------------------------------------------------------------------------------------------
-- (notcleaning)[invoicedate] has a viald value
-------------------------------------------------------------------------------------------
---(cleaning)deal with rows where [customerid] has null
select * from [clean].[OnlineRetail]
where  [customerid] is null                --133,702

update [clean].[OnlineRetail]
set[customerid] =right(abs(checksum(newid())), 5)     --133,702
where [customerid]  is null   
--------------------------------------------------------------------------------------------------------------------------
--(cleaning)[country] is cleaning Except for the customer has [customerid]=5388
select * from [clean].[OnlineRetail]
order by [country]
--------- custorm has [customerid]=5388 has invaild [customerid] , invaild [country] 
-----------and null for[invoicedate] ,null[quantity] and (invaild [invoiceno] , invaild[invoiceno]) i wirte these values 
----so i will delet this custormer

delete from [clean].[OnlineRetail]
where [customerid]= '95505' --1
-----------------------------------------------------------------------------

----Create views with transformations (helpers you’ll reuse)
-- All rows + create antor columns to help me 
-- reusable view: returns every row + helper columns
create or alter view clean.vw_Sales_All as
select
    r.*,
    case when left(r.invoiceno, 1) in ('c', 'C') then 'Return' else 'Sale' end as isreturn,
    try_convert(decimal(18,4), r.unitprice) * try_convert(decimal(18,4), r.quantity) as totalprice,
    convert(date, r.invoicedate) as invoicedateonly,   -- date without time
    format(r.invoicedate, 'yyyy-MM') as yearmonth,     -- e.g., 2021-04
    datepart(year,  r.invoicedate) as [year],
    datepart(month, r.invoicedate) as [month]
from clean.OnlineRetail as r;
go

----------------------------------
select * from clean.vw_Sales_All 
where [invoiceno]  not like'C%'

select * from clean.vw_Sales_All 
where [invoiceno]  like'C%'

select InvoiceNo, IsReturn, Quantity, UnitPrice
FROM clean.vw_Sales_All;
----------------------------------------------------------------------------------------------------------------------

select 
  InvoiceNo,
  case when LEFT(InvoiceNo,1) IN ('C','c') then 'Return' else 'Sale' end as IsReturn
FROM clean.vw_Sales_All;
---------------------------------------------------------------------
select * from[clean].[vw_Returns]
-----------------------------------------------------------------------------------------------------
-- valid sales only (for revenue analysis)
create or alter view clean.vw_sales_valid as
select *
from clean.vw_sales_all
where isreturn = 'Sale'
  and unitprice > 0
  and quantity  > 0
  and invoicedate is not null;
go                                        --524,878
---------------------------------------------------------------------------
-- returns only (useful for return-rate metrics)
create or alter view clean.vw_returns as
select *
from clean.vw_sales_all
where isreturn = 'Return';
go                            
-------------------------------------------------------------------------------------------------------------------
-- insights
--calculate the retention rate based on customers who made more one purchases .
-- Customer retention rate: Percentage of customers who have made more than 1 purchase
with CustomerPurchases AS (
    select CustomerID, count( DISTINCT InvoiceNo) AS Purchases
    from clean.vw_Sales_Valid
    group by CustomerID
)
select
    CAST(SUM(case when Purchases > 1 then 1 else 0 end) * 100.0 / count(*) AS DECIMAL(4,2)) AS RetentionRate --54.13
from CustomerPurchases;      

--------------------------------------------------(54.33)------------------------------------------------------------------
--calculate the retention rate based on customers who made multiple purchases over a specific period.
with CustomerPurchases AS (
    select CustomerID, count( DISTINCT InvoiceNo) AS Purchases
    from clean.vw_Sales_Valid
    where  MONTH(InvoiceDate) = 2
    group by CustomerID
)
select 
    CAST(SUM(case when Purchases > 1 then 1 else 0 end) * 100.0 / count(*) AS DECIMAL(5,2)) AS RetentionRate
from CustomerPurchases;

--------------------------------------------------(depend on time)------------------------------------------------------------------
--Time to Purchase (Average Days to First Purchase)
--Insight: How long does it take for new customers to make their first purchase after their initial visit?
--This will help analyze how quickly new customers convert into paying customers.
-- Average days to first purchase for the available data in Year2010_2011_fixed
-- average days to first purchase for the available data in year2010_2011_fixed
--with firstpurchase as (  
--    select customerid, min(invoicedate) as firstpurchasedate  -- customer ids and first purchase date
  --  from [clean].[OnlineRetail]  
  --  group by customerid 
--)  
--select avg(datediff(day, firstpurchasedate, '2011-7-31')) as avgdaystofirstpurchase  
-- we calculate the average days from the first purchase to 2011-12-31
--from firstpurchase;  -- from the firstpurchase data we created above    --94
-------------------------------------------------
--the difference in days between first and second purchase date
with PurchaseTimes AS (
    select CustomerID, 
           min(InvoiceDate) AS FirstPurchaseDate,
           (select min(InvoiceDate) 
            from clean.vw_Sales_Valid
            where CustomerID = p.CustomerID 
              AND InvoiceDate > min(  p.InvoiceDate)) AS SecondPurchaseDate
    from [clean].[OnlineRetail] p
    group by CustomerID
)
-- Now selecting the difference in days between first and second purchase date
select 
    CustomerID,
   abs( DATEDIFF(day, FirstPurchaseDate, SecondPurchaseDate)) AS DaysToSecondPurchase
from PurchaseTimes
where SecondPurchaseDate IS NOT NULL;  -- filter by customers who made a second purchase     



-----------------------------------------------------------------------------------------------------------
-- Churn Rate (Customers who stopped buying)
--Insight: Identify customers who have stopped purchasing. This can help focus on retention strategies.
--This query calculates customers who haven’t purchased in the last X days.
-- Churn rate: Customers who haven't purchased in the last 180 days
with LastPurchase AS (
    select CustomerID, max(InvoiceDate) AS LastPurchaseDate
    from [clean].[OnlineRetail]
    group by CustomerID
)
select
    cast(sum(case when DATEDIFF(day, LastPurchaseDate, '2011-12-31') > 180 then 1 else 0 end) * 100.0 / COUNT(LastPurchaseDate) AS DECIMAL(5,2)) AS ChurnRate
from LastPurchase;  --33.44

----------------------------------------------------------------------------------------------------
--Sales Efficiency by Day of the Week
--Insight: Which days of the week generate the most revenue or sales volume?
--This helps identify which days of the week are your busiest, and which need more focus.
-- Sales efficiency by day of the week (which day of the week generates the most revenue)
select datename(weekday, InvoiceDate) as Day, sum(TotalPrice) AS Revenue
from clean.vw_Sales_Valid
group by datename(weekday, InvoiceDate)
order by Revenue DESC; 

-------------------------------------------------------------------------------------
--Top 10 Products with Highest Return Rate
--Insight: Identify products with the highest return rates. This can help improve 
--product quality or address issues with specific products.

-- Top 10 products with highest return rate
-- Top 10 products with highest return rate
--WITH ProductReturns AS (
 --   SELECT 
  --      Description,
  --      SUM(CASE WHEN IsReturn = 'Return' THEN 1 ELSE 0 END) AS ReturnCount,
   --     COUNT(*) AS TotalSales
   -- FROM clean.vw_Sales_Valid
   -- GROUP BY Description
--)
--SELECT 
   -- Description,
   -- CAST(ReturnCount * 100.0 / NULLIF(TotalSales, 0) AS DECIMAL(5,2)) AS ReturnRate
--FROM ProductReturns
---- Limit to top 10 products
--OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;


----------------------------------------------------------------------------------------------------
-- Revenue by Hour of the Day
--Insight: Understand sales patterns throughout the day. This can help optimize staffing,
--delivery schedules, or promotional activities.
-----------------------------------------------------------------------------------------
-- Revenue by hour of the day
select datepart(hour, InvoiceDate) as  hour, sum(TotalPrice) as Revenue
from clean.vw_Sales_Valid
group by datepart(hour, InvoiceDate)
order by sum(TotalPrice)  ;

----------------------------------------------------------------------------------------------
--Average Time Between Purchases (Repeat Purchases)
--Insight: Calculate how long, on average, it takes for a customer to make a repeat purcha
---------------------------------------------------------------------------------------------------
-- Average time between repeat purchases
with RepeatPurchases AS (
    select CustomerID, DATEDIFF(day, min(InvoiceDate), max(InvoiceDate)) AS DaysBetween
    from clean.vw_Sales_Valid
   group by CustomerID
    having count(DISTINCT InvoiceNo) > 1
)
select AVG(DaysBetween) AS AvgTimeBetweenPurchases
from RepeatPurchases;
--------------------------------------------------------------------------------------- 180 days
-- Total sales per product category (if category column exists)
--Insight: Understand how sales are distributed across product categories.
--If your products are categorized, you can group them by category.
------------------------------------------------------------------------------------------
--Total sales per product category
select [stockcode], sum(TotalPrice) as Revenue
from clean.vw_Sales_Valid
group by [stockcode]
order by Revenue DESC;
------------------------------------------------------------------------------------------------
-- Cross-Selling Opportunities (Frequently Bought Together)
--Insight: Identify which products are often bought together. This can help with bundling and promotions.
---------------------------------------------------------
---------------products often bought together
with ProductPairs AS (
    select a.Description AS ProductA, b.Description AS ProductB, count(*) AS Frequency
    from clean.vw_Sales_Valid a
    join clean.vw_Sales_Valid b ON a.InvoiceNo = b.InvoiceNo AND a.Description != b.Description
    group by a.Description, b.Description
)
select ProductA, ProductB, Frequency
from ProductPairs
where Frequency>200
order by Frequency desc;                         --7,588,897

-------------------------------------------------------------------------------------------------
--monthly revenue trend (clean sales only)
--Insight: show seasonality and growth/decline over time
----------------------------------------------------------
--monthly revenu
select YearMonth, sum(TotalPrice) AS Revenue
from clean.vw_Sales_Valid
group by YearMonth
order by YearMonth;
--------------------------------------------------------------------------------
--Insight: your best market in country 
----------------------------------------------------------------------------
-- Revenue by country
select 
 case when Country IS NULL OR Country='' then 'Unknown' else Country end as CountryGroup,
  sum(TotalPrice) AS Revenue
from clean.vw_Sales_Valid
group by case when Country IS NULL OR Country='' then 'Unknown' else Country end
order by Revenue DESC;
-------------------------------------------------------------
-- Overall return rate
-----------------------------------------------------------------------------
select 
  cast(100.0 * sum(case when IsReturn='Return' then 1 else 0 end) / count([isreturn]) AS DECIMAL(5,2)) AS ReturnRatePct
from clean.vw_Sales_All;
--------------------------------------------------------------------------------------1.73
-- Month return rate
with base as (
  select YearMonth,
         sum(case when IsReturn='Return' then 1 else 0 end) AS Returns,
         count(*) AS Totalsales
  from clean.vw_Sales_All
  group by YearMonth
)
select YearMonth,
       Returns,
       Totalsales,
       cast(100.0*Returns/NULLIF(Totalsales,0) AS DECIMAL(5,2)) AS ReturnRatePct
from base
order by YearMonth;
--------------------------------------------------------------------------------------------------
-- Top customers (Monetary) + simple RFM base (Recency, Frequency, Monetary)
--The RFM system helps us identify our best customers based on:
--Insight: who your best customers are
declare @maxdate date = (select max(invoicedate) from clean.vw_sales_valid);

with cust as (
    select 
        customerid,
        count(distinct invoiceno) as frequency, 
        sum(totalprice) as monetary, 
        max(invoicedate) as lastpurchase
    from clean.vw_sales_valid
    where customerid is not null
    group by customerid
),
rfm as (
    select 
        customerid,
        datediff(day, lastpurchase, @maxdate) as recencydays,
        frequency,
        monetary,
        ntile(4) over (order by datediff(day, lastpurchase, @maxdate)) as r_score,
        ntile(4) over (order by frequency desc) as f_score,
        ntile(4) over (order by monetary desc) as m_score
    from cust
)
select *, (r_score*100 + f_score*10 + m_score) as rfm_code
from rfm
order by monetary desc;
---------------------------------------------------------
--Product performance (units vs revenue)
--Insight: volume vs value—some items sell many units but low revenue, and vice versa.
-----------------------------------------------
--Product performance
select
  coalesce(Description, '[No Description]') AS Product,  
  sum(Quantity) AS UnitsSold,   
  sum(TotalPrice) AS Revenue  
from clean.vw_Sales_Valid  
group by coalesce(Description, '[No Description]')  
order by Revenue DESC;  
-------------------------------------------------------------------------
-- Order KPIs 
--Insight: sales efficiency per order.
select  
  sum(TotalPrice)*1.0 / count(DISTINCT InvoiceNo) AS AvgOrderValue  --may be put this as kpi
from clean.vw_Sales_Valid; 
----------------------------------total Revenue ÷ Num Order------------------------------------------------------
--How many items are purchased per order
--------------------------------------------------------
--(units per order) 
select 
  sum(Quantity)*1.0 / nullif(count(DISTINCT InvoiceNo),0) AS AvgBasketUnits  --may be put this as kpi
from clean.vw_Sales_Valid; 
-----------------------------------------------total unties sold ÷ Num Order----------------------------------------
-------------------------






 




















