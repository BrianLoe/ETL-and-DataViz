# Data visualisations dashboards using Python Jupyter Notebook, Power BI, and Tableau

## 1. Hotel demand project using Python (on-going)
[Data source](https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand)  
This is an EDA project on hotel demand dataset with rich features. I implemented various data process here such as transforming data (cleaning and merging), data visualisation using matplotlib and seaborn and creating dashboard using Tableau. During the transformation phase, I used external data source for mapping country code to country name. Tableau was the preferred method to visualise the data here due to the number of features the data has.  
[Dashboard 1 link (Tableau)](https://public.tableau.com/app/profile/brian.loe4583/viz/Hotelbookingsdemand/Bookingsperformance)  
[Dashboard 2 link (Tableau)](https://public.tableau.com/app/profile/brian.loe4583/viz/Bookingsalesdetails/BookingSalesDetails)
### Overview and Methodology:
The data is about booking demand of a city hotel and resort hotel. The data was originally published in this [article](https://www.sciencedirect.com/science/article/pii/S2352340918315191). It includes 32 columns which encouraged me for doing data visualisation and exploration. The data cleaning phase was not too complicated as there are only a few null values, data is quite consistent, and date is in the right format. During the transformation phase I had to import external data source for alpha-3 country codes to map the original dataset to have country names instead of codes. Some interesting features are `Reservation Status`, `average daily rate (adr)`, `repeated guest`, `market segment`, `prev. cancellation`, `customer type`, `distribution channel`, `canceled bookings`, `deposit type`. 

###Insights:
Dashboard 1 (Hotel booking performance):
- Comparing the two hotels, city hotel has a lot of canceled reservation (27%) than resort hotel (9%).
- August is the peak month for customers to check-in their reservation for both hotels.
- Most of the customers are from Portugal and surrounding (mostly European countries).
- 

## 2. PWC Virtual Experience Dashboard
### Task 1: Call Centre Dashboard
The client from a call centre company wants to know if they are doing well handling customers' call. They have requested to be created a dashboard for key components of their call centre data. The client asks a dashboard to understand their data better.

Insights:
- Incoming calls mostly occured on Monday and Saturday around 11 AM - 12 PM and 2 PM - 3:30 PM
- The average answer speed is around 1 minute with 3 minutes 40 seconds talk duration
- Almost 75% of calls were resolved and 81% of calls were answered by the agents
- All agents performance on answering and resolving customer calls are similar
- The current satisfaction rating is 3.4/5

### Task 2: Customer Retention Dashboard
The client now wants to know what kind of strategies needed to keep the current customers. What kind of customers are influential and impactful to the business growth? and what are the reasons that customers churn?

Insights:
- On the demographics side, 31% of the customers are on their first year, 20% on their fifth year, and 15% on their sixth year
- Gender distribution is equal, half of the customers has partner, 16% are senior customers, 30% has dependents
- Out of approximately 7000 customers, 1869 of them are at risk of churning
- Monthly charges are on average about $65
- Almost 60% of customers opted in paperless billing
- Most of the customers opted in phone service (90%) followed by internet service (78%)
- Electronic check is the preferred method of payment (34%)
- More than half of the customers are on a monthly contract

Churn Analysis:
- Most churns occured on customers' first month of tenure and monthly customers which is reasonable and fine
- Customers that are on fiber optic monthly internet service are more likely to churn (55% of time) 
- Currently customers that have phone service are on risk of churn

### Task 3: Diversity & Inclusion (Human Resources) Dashboard
The client wants to see hiring, performance, promotion, and turnover of the company employees based on gender, age group, and department. Is diversity exist wihtin the company whether its regarding employees' age, gender, and job level? Does bias exist when i.e. hiring or promoting?

Insights:
- Since 2011-2017, hiring trends seems to have an inclination on male gender. However, this has since changed from 2018 where it can be seen that there is an equal opportunity for both genders to be hired
- There are 6 departments in the company, yet only Operations Department had an equal gender distribution. HR department is dominated by female and the 4 other departments is dominated by male
- New hires in Financial Year (FY) 20 were from the age group of 20-29 which means that there are opportunities for young generations to be hired
- Operations department hired the most new employess in FY20
- Overall, the current gender ratio 7:3 (Male:Female) which is still over the target of 5:5  

Performance, promotion, and turnover:
- Promotions were mostly made on male employees (78% vs 22%)
- The number of promotions are projected to be higher in FY21
- Average employee performance in FY19 and FY20 is similar although it saw a slight drop
- Turnovers are expected to be 87% in FY20, the leavers are confirmed to be 9.4%
- Turnovers will be massively expected to occur on Operations and Sales & Marketing Department
- Most of these turnovers will be employees on their third year

## 3. Premier League 21-22 clubs visualisation using Tableau
A. Do clubs that create a lot of chances placed higher in the standings?
Using goals/assists (G/A) per 90, their expected G/A (XG/XA) per 90, and non-penalty G/A & XG/XA to determine clubs' performance.
Chances are measured by clubs' ability to make chances and convert it into goals. It will be also determined with non-penalty to see if there is any difference.

Insights:
- Clubs that create a lot of chance actually do placed higher in the standing for the upper half table (1-10) 
- The bottom half (11-20) as can be seen, although they create many chances, they are being punished by not converting those into goals (i.e. the orange circle can be seen is above the blue circle) 
- Most clubs that are on the bottom half has only one particular player that contribute to the team goals (player that has higher than 20% goal contribution), they depend a lot on their key player

Goals win games and that applies to this premier league season. Man. City and Liverpool were amongst the top teams who converted a lot of goals from their chances. They were on their own league compared to the others. The under performance from the clubs can be seen through their chance conversion in the Non-penalty XG/XA vs actual G/A per 90 graph. This mostly reflects what happens in the table, with some clubs such as Brighton and Tottenham who were placed a lot higher despite having an under performance.

B. Is possession in premier league helps club win games or placed higher in the standings?
First comparison will be made on the top 6 teams which is Man. City, Liverpool,  Chelsea, Tottenham, Arsenal, and Man. United. These teams are called the Big Six in England. First of all, from these 6 teams, Man. City, Liverpool, and Chelsea has higher posessions than the others. To be able to score goals with possession, the most essential area of the pitch is the Attacking 3rd to Attacking penalty area of opposition. A higher number here will mean that the club is able to utilise their possession style of football to maximum. 

We can see that although the top 3 clubs has similar possession and touches, why does Chelsea has a huge gap points to the top 2 clubs?
From the posession vs touch area, we can see that Chelsea has the highest possession and touches in defensive 3rd area. This is not good because it means that Chelsea were not able to progress the ball into opponent half. With fewer touches, Liverpool and Man. City were able to rake more possession in those same area. 

Tottenham, Arsenal, and Man. United has similar pattern. If we compare the top 2 teams with the 4 other teams, it is obvious that a lot of touches in defensive 3rd is not good. We can see a big gap in attacking area possession vs touches. This might happen because they lack a player in defense and midfield that can progress the ball up higher into attacking half. Being stuck in defensive 3rd is not good because it means that they are wasting a lot of time on their own half which then few factors such as opponent pressing, defense low block, etc. will outplayed themselves.

## 4. KPMG Virtual Experience Dashboard
