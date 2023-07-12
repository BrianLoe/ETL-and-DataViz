# Data visualisations dashboards using Python Jupyter Notebook, Power BI, and Tableau

##### Data visualisations using Tableau: [DOTA 2 Meta Comparison](https://public.tableau.com/app/profile/brian.loe4583/viz/Dota2Meta/Dashboard1), [Din Tai Fung Sales Dashboard](https://public.tableau.com/app/profile/brian.loe4583/viz/SalesDashboardDTF/DinTaiFungOnlineOrderingDashboard)

## Premier League 21-22 clubs visualisation using Tableau
A. Do clubs that create a lot of chances placed higher in the standings?
Using goals/assists (G/A) per 90, their expected G/A (XG/XA) per 90, and non-penalty G/A & XG/XA to determine clubs' performance.
Chances are measured by clubs' ability to make chances and convert it into goals. It will be also determined with non-penalty to see if there is any difference.

Dashboard link: [Tableau Dashboard](https://public.tableau.com/app/profile/brian.loe4583/viz/PremierLeagueClubsVisualisation/Attackingvis)

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

## Implementing ETL pipeline on human resource dataset and data analytics
[Data source](https://github.com/Koluit/The_Company_Data)  
This project focuses on People / Human Resource analytics, demonstrating the implementation of an Extract, Transform, Load (ETL) process for data extraction, transformation, and loading into a PostgreSQL database. The project also involved data normalisation using a snowflake schema to ensure an efficient storage system. Additionally, Power BI was used for creating custom visualisations and interactive dashboards.

### Methodology:
1. ETL process.  
   I used Python notebook to do the ETL step of the project. This process is divided into extract, transform, and load stage.
   <div align="center"><img src="https://github.com/BrianLoe/ETL-and-DataViz/assets/58500773/8127227b-ef8d-4971-b445-03dfd84563b3" width=700></div>

   - Extract: There are 4 + 1 data that I identified and each correspond to the company, job details, demographic, and the full/main data. The +1 is a data        about survey results. We need to import those data into the notebook using pandas. The data can be either downloaded through Kaggle or the creator's          GitHub. This a relatively simple stage due to local storage. If we need to use an API such as REST API or even Kaggle API, we need to download the data from the web automatically using bash script.
     
   - Transform: The main transformations done here are converting data types, validating id for each data, validating integrity and accuracy, handling null values, and formatting dates variable. The steps taken here has to be in order. The idea is we need to clean the data with simpler structure.
     - First, we clean the company data. This is due to it having only 3 columns and it provides information on the office locations (1 change done).
     - Second step is to clean the job details data. It provides information about job profile, title and financial package that each employee receive (2 changes done).
     - Third is to clean full/main data. It provides the full information about each employee. This is the most crucial step because it consists of several violations of validity. Check for matching ids in other data, formatting date variable, checking correctness, etc. (9 changes done).
     - Fourth is to clean demographic data which provides demographic information on employees (1 change done).
     - Final step is to clean survey results data. It provides the result of a recent survey that the company held, overall the data provides response in rating out of 5.
     
   - Load: Finalise the transformation and create csv files for each data. We need to load the csv files inside a folder in the environment. In order to store the data into a SQL database, we need to ensure the data is relational. I also did normalisation to remove duplication and to provide efficient storage. Then we create a database in PostgreSQL. This can be done using PGadmin4 the GUI (which is easier) or through command prompt. Establish a connection to the created database using own credentials. Create tables for each data except survey data and add foreign keys accordingly. Insert the data into each tables using importing tools in GUI or command prompt.

2. Creating compelling visualisations using Power BI.
Power BI provides integration with PostgreSQL, so we can directly connect to our database. I created five different dashboards with each displaying different information. The dashboards are Employee's job related information, diversity info, wellbeing info, hiring info, and the survey results. Each dashboard is linked to each other. 

## Hotel demand project using Python
[Data source](https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand)  
This is an EDA project on hotel demand dataset with rich features. I implemented various data process here such as transforming data (cleaning and merging), data visualisation using matplotlib and seaborn and creating dashboard using Tableau. During the transformation phase, I used external data source for mapping country code to country name. Tableau was the preferred method to visualise the data here due to the number of features the data has.  
[Dashboard 1 link (Tableau)](https://public.tableau.com/app/profile/brian.loe4583/viz/Hotelbookingsdemand/Bookingsperformance)  
[Dashboard 2 link (Tableau)](https://public.tableau.com/app/profile/brian.loe4583/viz/Bookingsalesdetails/BookingSalesDetails)
### Overview and Methodology:
The data is about booking demand of a city hotel and resort hotel. The data was originally published in this [article](https://www.sciencedirect.com/science/article/pii/S2352340918315191). It includes 32 columns which encouraged me for doing data visualisation and exploration. The data cleaning phase was not too complicated as there are only a few null values, data is quite consistent, and date is in the right format. During the transformation phase I had to import external data source for alpha-3 country codes to map the original dataset to have country names instead of codes. Some interesting features are `Reservation Status`, `average daily rate (adr)`, `repeated guest`, `market segment`, `prev. cancellation`, `customer type`, `distribution channel`, `canceled bookings`, `deposit type`. 

### Insights:
Dashboard 1 (Hotel booking performance):
- Comparing the two hotels, city hotel has a lot of canceled reservation (27%) than resort hotel (9%).
- August is the peak month for customers to check-in their reservation for both hotels.
- Most of the customers are from Portugal and surrounding (mostly European countries).
- Highest average daily rate is achieved in August overall but City hotel achieved it in May.  

Dashboard 2 (Hotel booking sales):
- Overall, repeated guests come from Corporate market. However, between the two hotels, city hotel has a lof of repeated guests from Corporate while resort hotel has a lot of repeated guests from Direct (people ordering directly) segment. Online travel agency (TA) is also popular source.
- The number of customers that had previous cancellations are similar for Contract and Group type customers. However, for Transient (Regular customers) type, the number of customers that have not canceled is significantly larger compared to Transient-Group (Regular customers but in group) which has higher previous cancellations.
- Travel agency (TA)/Tour operators (TO) produce a lot of bookings compared to other distribution channels. City hotel has the most significant value for TA/TO channel.
- No deposit type was the most preferred option when making a reservation for both hotels.
- Transient customer type that canceled the booking and make no deposit represent about 80% of customers.

## PWC Virtual Experience Dashboard
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
