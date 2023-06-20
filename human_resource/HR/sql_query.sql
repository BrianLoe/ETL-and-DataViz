select j.department, round(avg("salary(usd)"),2), count(employeeid) as "No. of Employees"
from main_details m left join
job_details j on m.job_profile=j.job_profile
group by 1 
order by 2;

select j.level, count(employeeid) as "No. of Employees"
from main_details m left join
job_details j on m.job_profile=j.job_profile
group by 1 
order by 2;

select o.office_name, o.office_type, count(employeeid) as "No. of Employees"
from main_details m left join
office o on m.office_id=o.office_id
group by cube(1,2)
order by 1,3 desc;

select j.job_title, max("salary(usd)") as "Highest Salary"
from main_details m left join
job_details j on m.job_profile=j.job_profile
group by 1 
order by 2 desc
limit 10;