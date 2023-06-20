select j."Department", avg("Salary"), count("EmployeeID") as "No. of Employees"
from main_details m left join
job_details j on m."Job_Profile"=j."Job_Profile"
group by 1 
order by 2;

select j."level", count("EmployeeID") as "No. of Employees"
from main_details m left join
job_details j on m."Job_Profile"=j."Job_Profile"
group by 1 
order by 2;

select o."office_name", o."office_type", count("EmployeeID") as "No. of Employees"
from main_details m left join
office o on m."office_id"=o."office_id"
group by cube(1,2)
order by 1,3 desc;