with features as (
select 
 SKU_REVIEW_ID,
 avg(case when FEATURE_ID='HowWouldYouRateYourInstructorguideForThisClassactivity' then RATING end) as Instructor_Rating,
 avg(case when FEATURE_ID='WhatSkillLevelIsThisExperienceAppropriateFor' then RATING end) as Skill_Rating,
 avg(case when FEATURE_ID='DidThisExperienceMeetYourExpectations' then RATING end) as Expectations_Rating,
 avg(case when FEATURE_ID='HowWouldYouRateYourCourseLocation' then RATING end) as Location_Rating
 from "SKU_REVIEW_FEATURE_RATING"
 group by 1
) --d on a.SKU_REVIEW_KEY=d.SKU_REVIEW_KEY and a.SKU_REVIEW_ID=d.SKU_REVIEW_ID and a.PRODUCT=d.PRODUCT and a.SKU=d.SKU

,feature_instructor_range as (
select 
 SKU_REVIEW_ID,
 'Poor(1)-Excellent('||max(RATING_RANGE)||')' as Instructor_Rating_Range
  from (select SKU_REVIEW_ID,RATING_RANGE,FEATURE_ID, RATING from "SKU_REVIEW_FEATURE_RATING" where FEATURE_ID='HowWouldYouRateYourInstructorguideForThisClassactivity') a
 group by 1
)

,feature_location_range as (
select 
 SKU_REVIEW_ID,
 'Poor(1)-Excellent('||max(RATING_RANGE)||')' as Location_Rating_Range
  from (select SKU_REVIEW_ID,RATING_RANGE,FEATURE_ID, RATING from "SKU_REVIEW_FEATURE_RATING" where FEATURE_ID='HowWouldYouRateYourCourseLocation') a
 group by 1
)

,feature_skill_range as (
select 
 SKU_REVIEW_ID,
 min(LOW_RANGE_DESC)||'(1)-'||max(HIGH_RANGE_DESC)||'('||max(RATING_RANGE)||')' as Skill_Rating_Range
  from (select SKU_REVIEW_ID,RATING_RANGE,FEATURE_ID, RATING,LOW_RANGE_DESC,HIGH_RANGE_DESC from "SKU_REVIEW_FEATURE_RATING" where FEATURE_ID='WhatSkillLevelIsThisExperienceAppropriateFor') a
 group by 1
)

,feature_expectations_range as (
select 
 SKU_REVIEW_ID,
 min(LOW_RANGE_DESC)||'(1)-'||max(HIGH_RANGE_DESC)||'('||max(RATING_RANGE)||')' as Expectations_Rating_Range
  from (select SKU_REVIEW_ID,RATING_RANGE,FEATURE_ID, RATING,LOW_RANGE_DESC,HIGH_RANGE_DESC from "SKU_REVIEW_FEATURE_RATING" where FEATURE_ID='DidThisExperienceMeetYourExpectations') a
 group by 1
)

,tag_pro as (
select
e.SKU_REVIEW_ID,
listagg(e.TAG_TEXT, '; ') as Pros
from (select Distinct SKU_REVIEW_ID,SKU_REVIEW_TAG_DIMENSION_ID,SKU,TAG_TEXT from "SKU_REVIEW_TAG" where SKU_REVIEW_TAG_DIMENSION_ID='Pros') e
group by 1
) --e on a.SKU_REVIEW_ID=e.SKU_REVIEW_ID

,tag_con as (
select
e.SKU_REVIEW_ID,
listagg(e.TAG_TEXT, '; ') as Cons
from (select Distinct SKU_REVIEW_ID,SKU_REVIEW_TAG_DIMENSION_ID,SKU,TAG_TEXT from "SKU_REVIEW_TAG" where SKU_REVIEW_TAG_DIMENSION_ID='Cons') e
group by 1
)


,tag_why as (
select
e.SKU_REVIEW_ID,
listagg(e.TAG_TEXT, '; ') as Why_Attend
from (select Distinct SKU_REVIEW_ID,SKU_REVIEW_TAG_DIMENSION_ID,SKU,TAG_TEXT from "SKU_REVIEW_TAG" where SKU_REVIEW_TAG_DIMENSION_ID='WhyDidYouChooseToTakeAClassattendAnEventWithRei') e
group by 1
)

, tag as (
select
a.SKU_REVIEW_ID,
e.Pros,
f.Cons,
g.Why_Attend

from
(select distinct SKU_REVIEW_ID from "SKU_REVIEW") a
left join tag_pro e on a.SKU_REVIEW_ID=e.SKU_REVIEW_ID
left join tag_con f on a.SKU_REVIEW_ID=f.SKU_REVIEW_ID
left join tag_why g on a.SKU_REVIEW_ID=g.SKU_REVIEW_ID

) --e on a.SKU_REVIEW_ID=e.SKU_REVIEW_ID
  
, attendance as(
select cs.coursesessionid,
       count(distinct r.registrationid) as registration_count
from "COURSESESSION" cs
join "REGISTRATION" r using(coursesessionid)
left join "REGISTRATIONTRANSACTION" rt using(registrationtransactionid)
where date_part('year', STARTDTTM) >=2019
  and cs.statusid <> 4
  and r.seatstatusid = 1
  and rt.STATUSID = 1
group by coursesessionid
)  

select
current_date as refresh_date,
a.SKU_REVIEW_ID,
c.START_DATE_KEY,
c.START_DATETIME,
a.PRODUCT as CourseID,
c.PRODUCT_NAME as Course_Name,
a.SKU as SessionID,
c.SKU_NAME as Session_Name,
a.REVIEW_TITLE,
a.REVIEW_TEXT,
a.REVIEW_PRE_CLASS_ADDITIONAL_INFO_TEXT,
a.REVIEW_CLASS_CHANGE_SUGGESTION_TEXT,
a.REVIEW_RATING_NBR,
case when a.PRODUCT_RECOMMENDED_FLAG=1 then 'True'
        when a.PRODUCT_RECOMMENDED_FLAG=0 then 'False'
        else null end as PRODUCT_RECOMMENDED_FLAG,
a.REVIEW_CLASS_INSTRUCTOR_FEEDBACK_TEXT,
a.REVIEW_DEEP_LINKED_URL as Course_URL,

c.PROGRAM_NAME as Program,
c.SPONSOR_STORE_ID,
s.STORENBR,
c.SPONSOR_STORE_NAME,
c.MEETING_LOCATION_ID,
c.MEETING_LOCATION_NAME,
c.MARKET_ID,
c.MARKET_NAME,
case when c.MARKET_NAME is not null then c.MARKET_NAME
    when c.MARKET_NAME is null then c.SPONSOR_STORE_NAME end as MARKET_STORE_COMBO,
m.MARKETSIZEID,
case when (m.MARKETSIZEID=1  or c.SPONSOR_STORE_NAME is not null) then 'Small'
    when m.MARKETSIZEID=4 then 'X-Large'
    when m.MARKETSIZEID=3 then 'Large'
    when m.MARKETSIZEID=2 then 'Medium' end as Market_Size,
c.DISTRICT_ID,
c.DISTRICT_NAME,
c.PROGRAM_TYPE_CODE,
c.PROGRAM_TYPE_DESC,
t.registration_count,

d.Instructor_Rating,
z.Instructor_Rating_Range,
d.Skill_Rating,
x.Skill_Rating_Range,
d.Expectations_Rating,
w.Expectations_Rating_Range,
d.Location_Rating,
y.Location_Rating_Range,

e.Pros,
e.Cons,
e.Why_Attend

from
"ACTIVITY"."ACTIVITY"."SKU_REVIEW" a
inner join "PRODUCT"."PRODUCT"."OPO_SKU" c on a.SKU=c.SKU
left join features d on a.SKU_REVIEW_ID=d.SKU_REVIEW_ID

left join feature_instructor_range z on a.SKU_REVIEW_ID=z.SKU_REVIEW_ID
left join feature_location_range y on a.SKU_REVIEW_ID=y.SKU_REVIEW_ID
left join feature_skill_range x on a.SKU_REVIEW_ID=x.SKU_REVIEW_ID
left join feature_expectations_range w on a.SKU_REVIEW_ID=w.SKU_REVIEW_ID

left join tag e on a.SKU_REVIEW_ID=e.SKU_REVIEW_ID
left join "ERM"."ERM"."MARKET" m on c.MARKET_ID=m.MARKETID
left join "ERM"."ERM"."STORE" s on c.SPONSOR_STORE_ID=s.STOREID
left join attendance t on a.SKU=t.COURSESESSIONID
where a.Moderation_Status_Desc = 'APPROVED'