message(paste(timestamp(),"\nExecution of script <task1.R> started:"))

require("RODBC")
require("tidyverse")
require("assertthat")

# Recommended libraries:
library("moments");
library("lubridate")
library("forecast")

# Please make sure that you are in the working directory with the csv-datasets:
# If you are executing the script from a different directory, please set this to change into the directory with the CSV files: setwd("directory") 
source("libsubmission.R")

# Initialise submission:
submission_initialise()

# Please put the code to connect to your database here:
ch_infombin <- odbcConnect("covid", uid= , pwd= , case="postgresql") # TODO <<< This needs to be changed prior to the first run! THE FIRST RUN
assert_that(ch_infombin!=-1, msg = "ODBC connection failed. Please adjust the odbcConnect() - call in the 'task1.R' script prior to executing it.")



##############################################

### Extract, Transform, Load ###

## (a) Loading and cleansing of each source table into a staging table

#unique is used to ensure that no duplicate rows are retrieved
casesperson <- unique(read_csv("casesperson.csv")) # TODO
casesmedrec <- unique(read_csv("casesmedrec.csv")) # TODO
casestravel <- unique(read_csv("casestravel.csv")) # TODO
geomapping <- unique(read_csv("geomapping.csv")) # TODO

#remove rownames column
casesperson[,1] <- {} 
casesmedrec[,1] <- {}
casestravel[,1] <- {}
geomapping[,1] <- {}

# Age  < 0 (negative age)is replace with NA's
casesperson$age[casesperson$age < 0 ] = NA

#Saving cleaned dataframes to db
sqlSave(channel=ch_infombin,dat=casesperson,tablename="casesperson",append=FALSE,fast=FALSE)
sqlSave(channel=ch_infombin,dat=casesmedrec,tablename="casesmedrec",append=FALSE,fast=FALSE,varTypes=c(date="date"))
sqlSave(channel=ch_infombin,dat=casestravel,tablename="casestravel",append=FALSE,fast=FALSE,varTypes =c(flight_departure_date="date",flight_arrival_date="date") )
sqlSave(channel=ch_infombin,dat=geomapping,tablename="geomapping",append=FALSE,fast=FALSE)

# Question: Provide a cleansed version of the data in casesperson.csv as a data frame with column names 'cid','residence_code','residence_name','gender','age', with the rows ordered by attribute 'cid':
save_answer("1",casesperson) # TODO
# As a backup, this answer is also saved to the file "casesperson.RData", which will be included in the submission:
saveRDS(casesperson,file="casesperson.RData")



# Question: Provide a cleansed version of the data in casesmedrec as a data frame with column names 'cid', 'date', 'has_fever', 'has_cough', 'has_fatigue', 'has_dyspnea', 'has_anorexia', 'has_anosmia', 'has_obesity', 'status', 'hospital_admission', with the rows ordered by attribute 'cid':
save_answer("2",casesmedrec) # TODO
# As a backup, this answer is also saved to the file "casesmedrec", which will be included in the submission:
saveRDS(casesmedrec,file="casesmedrec.RData")



# Question: Provide a cleansed version of the data in casestravel as a data frame with column names 'tid', 'cid', 'last_country', 'flight_number', 'flight_departure_date', 'flight_arrival_date', with the rows ordered by attribute 'cid'::
save_answer("3",casestravel) # TODO
# As a backup, this answer is also saved to the file "casestravel", which will be included in the submission:
saveRDS(casestravel,file="casestravel.RData")



# Question: Provide a cleansed version of the data in geomapping as a data frame with column names 'dhb2015_name', 'dhb2015_code', 'au2017_code', 'au2017_name', with the rows ordered by attribute 'au2017_code'::
save_answer("4",geomapping) # TODO
# As a backup, this answer is also saved to the file "geomapping", which will be included in the submission:
saveRDS(geomapping,file="geomapping.RData")



# Question: Provide a SQL query that selects all attributes of a case into a single flat table. This table must include each attribute exactly once, and in the following order: 'cid','gender','age','residence_code','residence_name','dhb2015_code','dhb2015_name','tid','last_country','flight_number','flight_departure_date','flight_arrival_date','date','has_fever','has_cough','has_fatigue','has_dyspnea','has_anorexia','has_anosmia','has_obesity','status','hospital_admission'
# The rows must be in ascending order of 'cid'.  
#query_text_copy_flat_table <- paste("SELECT * INTO flattable FROM (",substr(query_text_flat_table,start = 1, stop = str_length(query_text_flat_table)-1),") AS tmp;",sep = "")
query_text_flat_table <- "SELECT DISTINCT cp.cid, cp.gender, cp.age, cp.residence_code, cp.residence_name, gm.DHB2015_code, gm.DHB2015_name, ct.tid, ct.last_country, ct.flight_number, ct.flight_departure_date, ct.flight_arrival_date, cm.date, cm.has_fever, cm.has_cough, cm.has_fatigue, cm.has_dyspnea, cm.has_anorexia, cm.has_anosmia, cm.has_obesity, cm.status, cm.hospital_admission  FROM casesperson AS cp LEFT JOIN geomapping AS gm ON gm.AU2017_code =  cp.residence_code LEFT JOIN casestravel AS ct ON ct.cid = cp.cid LEFT JOIN casesmedrec AS cm ON cm.cid = cp.cid ORDER BY cp.cid;" # TODO
save_answer("5a",query_text_flat_table)

# Save this table to a data frame flat_table:
flat_table <- sqlQuery(channel=ch_infombin,query=query_text_flat_table) # Result of Extract
save_answer("5b",flat_table)
# As a backup, this answer is  also saved to the file "flattable.RData", which will be included in the submission:
saveRDS(flat_table,file="flattable.RData")

sqlSave(ch_infombin,dat=flat_table,tablename = "flattable",append=FALSE,fast=FALSE, varTypes = c(date="date"))

# In preparation for the next questions, save the result of the query above in a table named "flattable" in the database, and execute the queries below on that table:
# Hint: You might do this either directly from within the database with a SQL statement, or via sqlSave.
query_text_copy_flat_table <- "SELECT * FROM flattable;" # TODO
sqlQuery(channel=ch_infombin,query_text_copy_flat_table)
# Writing the content of table flattable in the database to a CSV file of that name, which will be included in the submission:
sqlQuery(channel=ch_infombin,paste("COPY flattable TO '",file.path(getwd(),"flattable.csv"),"' csv header;",sep=""))




##############################################

### DB Querying and Descriptive Analytics ###

## The first set of question on DB querying and descriptive analytics build on the previous results, and therefore require that you have created and populated the tables as described above.
## However, there is a second set of questions further below, which will use a different table (provided with the zip-archive) 'cases'. This is to ensure that students who did not succeed in the steps above can still obtain partial credit for some questions.

### Part 1  ###


# Question: How many cases are there in total?  First, provide a SQL statement for that query, then the result of the query as a data frame with column "count":
query_text_6 <- "SELECT COUNT(status) AS count FROM flattable WHERE status='confirmed' OR status='probable' ;" # TODO
save_answer("6a",query_text_6)
query_result_6 <- sqlQuery(channel=ch_infombin,query=query_text_6)
save_answer("6b",query_result_6)



# Question: When did the first case occur, when the last? First, provide a SQL statement for that query, then the result of the query as a data frame with columns "first" and "last":
query_text_7 <- "SELECT MIN(date) AS first, MAX(date) as last FROM flattable;" # TODO
save_answer("7a",query_text_7)
query_result_7 <- sqlQuery(channel=ch_infombin,query=query_text_7)
save_answer("7b",query_result_7)



# Question: For how many cases is the gender missing? First, provide a SQL statement for that query, then the result of the query as a data frame with column "count":
query_text_8 <- "SELECT COUNT(*) AS count FROM flattable WHERE gender IS NULL ;" # TODO
save_answer("8a",query_text_8)
query_result_8 <- sqlQuery(channel=ch_infombin,query=query_text_8)
save_answer("8b",query_result_8)



# Continuing the question from above, query an ordered list of the IDs of cases with missing values in the gender attribute (ordered by 'cid' in ascending order). First, provide a SQL statement for that query, then the result of the query as a data frame with column 'cid':
query_text_9 <- "SELECT cid AS cid FROM flattable WHERE gender IS NULL ORDER BY cid ;" # TODO
save_answer("9a",query_text_9)
query_result_9 <- sqlQuery(channel=ch_infombin,query=query_text_9)
save_answer("9b",query_result_9)



# Question: Give an ordered list of the case IDs of cases without a travel record (in ascending order). First, provide a SQL statement for that query, then the result of the query as a data frame with column "cid":
query_text_10 <- "SELECT cid FROM flattable WHERE tid IS NULL ORDER BY cid ;" # TODO
save_answer("10a",query_text_10)
query_result_10 <- sqlQuery(channel=ch_infombin,query=query_text_10)
save_answer("10b",query_result_10)




# Question: How many different countries are there in last_country (after cleansing), including NA? First, provide a SQL statement for that query, then the result of the query as a data frame with column "country":
query_text_11 <- "SELECT DISTINCT last_country AS country FROM flattable;"# TODO
save_answer("11a",query_text_11)
query_result_11 <- sqlQuery(channel=ch_infombin,query=query_text_11)
save_answer("11b",query_result_11)



# Question: Rank the countries in which New Zealanders most likely contracted the virus. The result should be a table with two columns, country (distinct values from last_country) and casecount, ordered (ascending order) by casecount. Again, provide the SQL statement for that query first, and then the resulting data frame:
query_text_12 <- "SELECT last_country AS country, COUNT(status) AS casecount FROM flattable  GROUP BY last_country ORDER BY casecount;" # TODO
save_answer("12a",query_text_12)
query_result_12 <- sqlQuery(channel=ch_infombin,query=query_text_12)
save_answer("12b",query_result_12)



# Question: How many distinct combinations of DHB2015_code and AU2017_code exist in geomapping (after cleansing)? First, provide a SQL statement for that query, then the result of the query as a data frame with columns "dhb2015_code", "au2017_code":
query_text_13 <- "SELECT DISTINCT DHB2015_code, residence_code AS au2017_code FROM flattable  ;" # TODO
save_answer("13a",query_text_13)
query_result_13 <- sqlQuery(channel=ch_infombin,query=query_text_13)
save_answer("13b",query_result_13)



##############################################

##############################################

FEYI FILL THIS UP!!

# Data Dictionary for table 'flattable' 

#

# Variable 'cid':

#    Meaning: 

#    Type and range of values: 

#    Missing (or special) values: 

#    Data source(s):

#    Transformation:

#    Known issues: 

#

# Variable 'gender'

# ...




##############################################

### DB Querying and Descriptive Analytics ###

## For the following queries and analyses, use the table 'cases'. Do *not* use flattable. By using 'cases', it is ensured that your answers to the questions below will be independent of the results of the data cleansing in the previous questions. While the table 'cases' has been built from a different dataset, it contains a subset of attributes similar to the ones in 'flattable' above (with some small differences, which should not influence your queries below), and you can imagine that it has been made analytics ready before being loaded into the warehouse' database. The table 'cases' contains the attributes 'cid','gender','age','dhb2015_code','dhb2015_name','tid','date','status', and the rows are in ascending order of 'cid'.


tables <- sqlTables(ch_infombin)
if (is.element(tolower("cases"),tables$TABLE_NAME)) sqlQuery(ch_infombin, "DROP TABLE cases;")
qdf <- sqlQuery(channel=ch_infombin,query = "CREATE TABLE public.cases (
  cid double precision,
  gender character varying(255),
  age double precision,
  dhb2015_code double precision,
  dhb2015_name character varying(255),
  date date,
  status character varying(255)
);")
assert_that(qdf == "No Data",msg="Creation of table cases has not worked. Please check.")
qdf <- sqlQuery(channel=ch_infombin,paste("COPY cases FROM '",file.path(getwd(),"cases.csv"),"' csv header;",sep=""))
assert_that(all(is.character(qdf),length(qdf) == 0),msg="Populating of table cases has not succeeded. Please check.")
cases <- sqlFetch(channel=ch_infombin, sqtable = 'cases')
assert_that(identical(names(cases),c("cid","gender","age","dhb2015_code","dhb2015_name","date","status")),msg="Something went wronog when populating/fetching the table cases. Please check.")


### Part 2: Querying  ###


# Question: Query counts of cases on variable day and DHB region (for simplicity, for now with omitting combinations of day and DHB region without any reported case): First, provide a SQL statement for that query, then the result of the query as a data frame with columns "date", region", "casecount", where rows are ordered first by "date" and second by "region":
query_text_14 <- "SELECT date, dhb2015_name AS region, COUNT(status) AS casecount FROM cases WHERE status IS NOT NULL GROUP BY date, region ORDER BY date, region;" # TODO
save_answer("14a",query_text_14)
query_result_14 <- sqlQuery(channel=ch_infombin,query=query_text_14)
save_answer("14b",query_result_14)



# Question: Create a table that contains for all possible combinations of regions (any distinct value in DHB2015_code) and dates (any date between the first and last day a case has been recorded in any region) the according case counts. That is, this table should have three columns region,date,casecount, ordered in ascending order on the first and second column. The third column gives a count over the cases in the corresponding DHB region and day, and zero if no such case was recorded in that region and day. 
# Hint 1: If you implement this in a single SQL query, a CROSS JOIN might be usefull. 
# Hint 2: In PostgreSQL, you can generate a series of days between a starting and end date with SELECT generate_series(startdate, enddate, interval '1 day');
# Furthermore, check your result for consistency (e.g., that all possible dates are included, and that all distinct regions that you find in the geomapping databases are present, and that the sum over the third column is equal to the count obtained in the very first query, where you counted all cases in the database

# First, provide a SQL statement for that query, then the result of the query as a data frame with columns "region", date", "casecount":
query_text_15 <- "with combo as (select distinct cs1.dhb2015_code, day  from cases cs1 cross join  generate_series('2020-03-25', '2020-04-21', interval '1 day') as gs(day) order by cs1.dhb2015_code, day ) select combo.dhb2015_code, combo.day,count(cs.status) as casecount from combo left join cases cs ON combo.dhb2015_code = cs.dhb2015_code AND combo.day <= cs.date AND( combo.day + interval '1 day') > cs.date group by combo.dhb2015_code, combo.day order by combo.dhb2015_code, combo.day  ;" # TODO
save_answer("15a",query_text_15)
query_result_15 <- sqlQuery(channel=ch_infombin,query=query_text_15)
names(query_result_15)[names(query_result_15) == "dhb2015_code"] <- "region"
names(query_result_15)[names(query_result_15) == "day"]  <- "date"
save_answer("15b",query_result_15)



# Question: Return a table that provides for each age group (in decades, starting with group 0 for ages 0-9, 10 for ages 10-19, etc.) the count of cases. This table should have two columns: agegroup, with values 0,10,20, etc., and casecount, with the counted number of cases. The rows in this table should be ordered ascendingly by age group.  Again, provide the SQL statement for that query first, and then the resulting data frame:
# Hint: Make sure that you include at the end a row with the cases who have missing values for age.
query_text_16 <- "SELECT  CASE WHEN age BETWEEN 0 AND 9 THEN 10  WHEN age BETWEEN 10 AND 19 THEN 20  WHEN age BETWEEN 20 AND 29 THEN 30 WHEN age BETWEEN 30 AND 39 THEN 40 WHEN age BETWEEN 40 AND 49 THEN 50 WHEN age BETWEEN 50 AND 59 THEN 60 WHEN age BETWEEN 60 AND 69 THEN 70 WHEN age BETWEEN 70 AND 79 THEN 80 WHEN age BETWEEN 80 AND 89 THEN 90 WHEN age BETWEEN 90 AND 99 THEN 100 ELSE null END AS agegroup, count(status) AS casecount from cases group by agegroup order by agegroup;" # TODO
save_answer("16a",query_text_16)
query_result_16 <- sqlQuery(channel=ch_infombin,query=query_text_16)
save_answer("16b",query_result_16)



# Question: Return a table that ranks weeks by their case counts. The first column, weeknumber, contains the week numbering (ISO 8601, Monday as first day of a week), the second column, casecount, the corresponding number of cases. The rows should be in descending order of case counts. Again, provide the SQL statement for that query first, and then the resulting data frame:
# Hint: This table needs only to contain weeks where at least one case has been recorded.
query_text_17 <- "select  DATE_PART('isodow', cs1.date) as weeknumber, count(cs1.status) as casecount from cases cs1  group by weeknumber order by casecount desc;" # TODO
save_answer("17a",query_text_17)
query_result_17 <- sqlQuery(channel=ch_infombin,query=query_text_17)
save_answer("17b",query_result_17)



# Question: Provide a table that lists all cases in a specific DHB region 3 (Auckland). Provide first a query, then the resulting data frame, which should contain the case IDs of all cases in that region, in ascending order, as column "cid":
query_text_18 <- "select cid from cases where dhb2015_code = 3 and status is not null order by cid;" # TODO
save_answer("18a",query_text_18)
query_result_18 <- sqlQuery(channel=ch_infombin,query=query_text_18)
save_answer("18b",query_result_18)



# Question: Create a slice that shows the number of cases in a specific DHB region 3 (Auckland) on the variables date and age. The result should be a table with three columns: date, age, and casecount. The entries in this table should be ordered first by date, then by age. As before, provide the SQL statement for that query first, followed by the resulting data frame:
query_text_19 <- "select date, age, count(status) as casecount from cases where dhb2015_code = 3 and status is not null group by date,age order by date,age;" # TODO
save_answer("19a",query_text_19)
query_result_19 <- sqlQuery(channel=ch_infombin,query=query_text_19)
save_answer("19b",query_result_19)



##############################################

### Part 3: Analysis within R  ###


# For the last questions, a table with casecounts for all possible dates (in range) is given: 
casesperday <- readRDS(file="casesperday.RData")
assert_that(identical(names(casesperday),c("date","casecount")),msg="Loading of casesperday from file <casesperday.RData> did not work. Please check.")


# Question: What is the mode of the variable cases$dhb2015_name? Provide a single number:
answer_20 = NA  # TODO
save_answer("20",answer_20)



# Question: Provide a list with 5 elements corresponding to the minimum, 1st quartile, median, 3rd quartile, maximum of casesperday$casecount.
answer_21 = list(min=NA, q1=NA, med=NA, q3=NA, max=NA) # TODO
save_answer("21",answer_21)



# Question: Provide a data frame with two columns: 'week' as the week number (ISO 8601 standard, for all weeks between the first and last case in casesperday)  in ascending order, then 'casecount' as the means for the daily cases that week. 
answer_22=data.frame(week=NA, casecount=NA) # TODO
save_answer("22",answer_22)



# Question: Create a plot time versus new cases with title "Daily New Cases". On the horizontal axis, it should show the date, while on the vertical axis the number of new cases per day. This plot should show the cases per day (denoted as "daily reported cases" in the legend) as dots, as well as the weekly average (denoted as "weekly average") as line. 

# First, create for each curve's coordinate on the horizontal (time) axis a Date object:
answer_23_x1 = as.Date(NA) # TODO
answer_23_x2 = as.Date(NA) # TODO
save_answer("23a",answer=answer_23_x1)
save_answer("23b",answer=answer_23_x2)

# Second, create for each curve's coordinate on the vertical (count) axis a vector:
answer_23_y1 = as.numeric(NA) # TODO
answer_23_y2 = as.numeric(NA) # TODO
save_answer("23c",answer=answer_23_y1)
save_answer("23d",answer=answer_23_y2)

# Third, put your code for plotting between the pdf() and dev.off() commands below to plot the result into the file "answer_39e.pdf".
filename = "answer_23e.pdf"
pdf(file=filename,width=12,height=9); 

# put your plotting code here, between pdf() and dev.off()
plot(x=0,y=0) # TODO
# ...
dev.off();
save_answer("23e",answer=filename,is_file=TRUE)




##############################################

# Close database connection:
odbcClose(channel=ch_infombin)

# Finalise submission:
submission_finalise()

# End notification:
message(paste(timestamp(),"\nExecution of script <task1.R> completed."))
#EOF