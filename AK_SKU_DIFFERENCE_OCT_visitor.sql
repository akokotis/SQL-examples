create or replace view "AK_SKU_DIFFERENCE_OCT_visitor" as (

---------------------
----Identify Bots
---------------------
with bots as (
select SESSION_KEY
  from CLICKSTREAM_SESSION_BOT
  where SESSION_BOT_FLAG = 1
  AND SESSION_START_DATE between '2020-10-06' and '2020-10-21'
  group by 1
)
  
---------------------
----Test Traffic
---------------------
, test_visitors as (
SELECT
hd.VISITOR_ID,
hash(VISITOR_ID) as VISITOR_KEY,
CASE WHEN POST_EVAR43 LIKE 'SkuAvailabilityControl' THEN 'CONTROL'
		WHEN POST_EVAR43 LIKE 'SkuAvailabilityTest' THEN 'TEST'
		ELSE null END AS EXPERIENCE,
MIN(DATE_TIME) AS FIRST_TEST_DATE_TIME
FROM
"HIT_DATA" hd
WHERE 1=1
and EXCLUDE_HIT<= 0
and HIT_SOURCE not in (5,7,8,9)
and hd.DATE_KEY between 20201006 and 20201021			
and POST_EVAR43 LIKE 'SkuAvailability%'
GROUP BY 1,2,3
)

,dups as (
select visitor_key,
	count(distinct experience) as multi
from test_visitors
group by 1
)


,AK_SKU_OCT20_TEST as (
SELECT distinct
a.VISITOR_key,
a.experience,
a.FIRST_TEST_DATE_TIME
FROM test_visitors a
JOIN dups b on a.VISITOR_key=b.VISITOR_key
WHERE b.multi = 1
)
  
  
--------------------------------
----Join to Search
--------------------------------
,clickstream_searches as (
  select distinct
  a.visitor_key,
  a.session_key,
  a.CLICKSTREAM_VISIT_PAGE_NUM,
  a.page_name_key,
  a.SEARCH_PAGE_NUM,
  SPLIT_PART(a.page_name, ':', 3) as search_term,
  a.SESSION_START_DATE_KEY,
  a.SESSION_PAGEVIEW_DATETIME,
  a.page_name,
  a.page_url,
  replace(a.product_list,';','') as PROD_LIST,
  c.experience,
  c.FIRST_TEST_DATE_TIME,
  s.DEVICE_TYPE,
  case when (s.SEARCH_REFINEMENT_TYPE='deals') then 'Deals Facet' when (s.page_name like '%deals%') then 'Deals Search' else s.NAVIGATION_CATEGORY_TYPE end as NAV_CATEGORY_TYPE
  
  from (select * from "SESSION_PAGEVIEW" where (page_name like 'nav_search%' or page_name like 'user_search%')
          and SESSION_START_DATE_KEY between 20201006 and 20201021) a 
  inner join AK_SKU_OCT20_TEST c on a.visitor_key=c.visitor_key and c.FIRST_TEST_DATE_TIME<=a.SESSION_PAGEVIEW_DATETIME
  Inner join (select * from "SESSION_SEARCH" where SESSION_SEARCH_DATE_KEY between 20201006 and 20201021 and SEARCH_SORT_TYPE='default') s on a.session_key=s.session_key and a.page_name_key=s.page_name_key and a.CLICKSTREAM_VISIT_PAGE_NUM=s.CLICKSTREAM_VISIT_PAGE_NUM and SPLIT_PART(a.page_name, ':', 3)=s.NAVIGATION_SEARCH_TERM and a.SEARCH_PAGE_NUM=s.SEARCH_PAGE_NUM
  LEFT JOIN bots b on a.session_key=b.session_key
  
  where 1=1
  and b.session_key is null
)

 
-------------------------
----PDP, ATC, PDP Exit
-------------------------
,PDP_view as (
select
  a.visitor_key,
  a.SESSION_PAGEVIEW_DATETIME, --datetime of the PDP view
  a.previous_page_name,
  a.Previous_page_name_key,
  a.page_name,
  a.product as PDP_product, --Product of this PDP view
  b.product as ATC_product, ---ATC of product anytime after PDP view
  case when e.EXIT_PAGE_KEY is not null then 1 else 0 end as pdp_exit_flag, ---exit on this specific PDP view
  row_number() over(partition by a.visitor_key, a.page_name,a.product,b.product order by a.SESSION_PAGEVIEW_DATETIME desc) as recency
  from (select * from "SESSION_PAGEVIEW" 
            where (PREVIOUS_PAGE_NAME like 'nav_search%' or PREVIOUS_PAGE_NAME like 'user_search%')
            and page_template='prod_page'
            and SESSION_START_DATE_KEY between 20201006 and 20201021) a
  left join (select *,substr(PRODUCT_LIST,2) as product
             from "SESSION_LINK_CLICK"
                where (SESSION_LINK_CLICK_NAME like 'pdp_add to cart%'
                and SESSION_LINK_CLICK_DATE_KEY	 between 20201006 and 20201021) b on a.visitor_key=b.visitor_key and a.product=b.product and b.SESSION_LINK_CLICK_DATETIME>a.SESSION_PAGEVIEW_DATETIME
  
  LEFT JOIN (select * from "CLICKSTREAM"."CLICKSTREAM"."SESSION_CUSTOMER" where SESSION_START_DATE_KEY BETWEEN  20201006 and 20201021) e on e.EXIT_PAGE_KEY=a.page_name_key and e.session_key=a.session_key
  group by 1,2,3,4,5,6,7,8
)


-------------------------
----Order
-------------------------  
, orders as (
select
  visitor_key,
ORDER_DATE_KEY, 
ORDER_DATETIME,  --datetime of the product order
product as ordered_product,
sum(ENTERPRISE_DEMAND_NET_AMT*CUSTOMER_DEMAND_LINE_QTY)  as demand,
sum(CUSTOMER_DEMAND_COGS_AMT*CUSTOMER_DEMAND_LINE_QTY) as cogs
from
"DIGITAL_DEMAND"
where ORDER_DATE_KEY between 20201006 and 20201021
group by 1,2,3,4
)


-------------------------
----Combined
-------------------------
,pdp_atc_order as (
select
  c.visitor_key,
  c.SESSION_PAGEVIEW_DATETIME, --datetime of the PDP view
  c.previous_page_name,
  c.Previous_page_name_key,
  c.page_name,
  
  c.PDP_product, --PDP views associated with the search term & results that happened anytime after the search datetime
  c.pdp_exit_flag, --PDP exit associated with the PDP view tied to search term & results that happened anytime after the search datetime
  c.ATC_product, --ATC of product that happened anytime after the PDP view
  d.ordered_product, --Ordered product associated with a PDP view that happened anytime after the view
  d.demand, -- demand of ordered product
  d.cogs, --- cost of goods of ordered product
  row_number() over(partition by c.visitor_key, c.page_name,c.PDP_product order by c.SESSION_PAGEVIEW_DATETIME desc) as order_recency
  from
 PDP_view c
  left join orders d on c.visitor_key=d.visitor_key and c.PDP_product=d.ordered_product and d.ORDER_DATETIME>c.SESSION_PAGEVIEW_DATETIME and c.recency=1
)


, search_product_list as (
select distinct
  a.visitor_key,

  a.SESSION_START_DATE_KEY,
  a.SESSION_PAGEVIEW_DATETIME, --datetime of the search

  a.SEARCH_PAGE_NUM,
  a.page_name,
  a.page_name_key,

  a.experience,
  a.FIRST_TEST_DATE_TIME,
  a.DEVICE_TYPE,
  a.NAV_CATEGORY_TYPE as NAVIGATION_CATEGORY_TYPE,
  
  a.prod_list,
  c.PDP_product, --PDP views associated with the search term & results that happened anytime after the search datetime
  c.pdp_exit_flag, --PDP exit associated with the PDP view tied to search term & results that happened anytime after the search datetime
  c.ATC_product, --ATC of product that happened anytime after the PDP view
  c.ordered_product, --Ordered product associated with a PDP view that happened anytime after the view
  c.demand, -- demand of ordered product
  c.cogs, --- cost of goods of ordered product
  case when e.EXIT_PAGE_KEY is not null then 1 else 0 end as search_exit_flag, --exit associate with this specific search datetime
  case when c.PDP_product is not null then row_number() over(partition by a.visitor_key, a.experience,a.DEVICE_TYPE,c.PDP_product order by a.SESSION_PAGEVIEW_DATETIME desc) end as search_recency
  from clickstream_searches a 
  LEFT JOIN pdp_atc_order c on a.visitor_key=c.visitor_key and a.page_name_key=c.Previous_page_name_key and contains(a.prod_list,c.PDP_product)='TRUE' and c.SESSION_PAGEVIEW_DATETIME>a.SESSION_PAGEVIEW_DATETIME and c.order_recency=1
  LEFT JOIN (select * from "SESSION_CUSTOMER" where SESSION_START_DATE_KEY BETWEEN  20201006 and 20201021) e on e.EXIT_PAGE_KEY=a.page_name_key and e.session_key=a.session_key
)

----------------------
----Search Impact Flag
-----------------------
, impact_flag as (
select
  distinct s.VISITOR_KEY
from SKU_AVAIL_TEST_OCT20_SEARCHES_DIFF d
    , SKU_AVAIL_TEST_OCT20_SEARCHES s
where 1=1
    and d.SESSION_SEARCH_DATE = s.SESSION_SEARCH_DATE
    and d.PAGE_URL = s.PAGE_URL
    and d.PAGE_TEMPLATE = s.PAGE_TEMPLATE
    and d.SEARCH_TERM = s.SEARCH_TERM
    and d.BREADCRUMB = s.BREADCRUMB
    and d.SEARCH_RESULTS_COUNT = s.SEARCH_RESULTS_COUNT
    and d.SAME_TOP_ROWS_FLAG = 0
    and d.FIRST_SLOT_FIRST_ROW_FLAG + d.SECOND_SLOT_FIRST_ROW_FLAG + d.THIRD_SLOT_FIRST_ROW_FLAG + d.FOURTH_SLOT_SECOND_ROW_FLAG + d.FIFTH_SLOT_SECOND_ROW_FLAG + d.SIXTH_SLOT_SECOND_ROW_FLAG < 6
)
  
--, combined as (
select distinct
a.VISITOR_KEY,
a.experience,
a.DEVICE_TYPE,
a.NAVIGATION_CATEGORY_TYPE,
case when b.VISITOR_KEY is not null then 1 else 0 end as top_two_rows_impact_flag,
  count(distinct PDP_product) as pdp_count,
  count(distinct ATC_product) as atc_count,
  count(distinct ordered_product) as ordered_product_count,
  sum(search_exit_flag) as exit_count,
  sum(pdp_exit_flag) as pdp_exit_count,
  sum(demand) as demand,
  sum(cogs) as cogs  
  from search_product_list a
  left join impact_flag b on a.VISITOR_KEY=b.VISITOR_KEY
  where search_recency is null or search_recency=1
  group by 1,2,3,4,5
);

