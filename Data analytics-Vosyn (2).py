# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
df1 = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("dbfs:/FileStore/shared_uploads/sureshsindhuja001@gmail.com/breezy_candidates__2_.csv")
display(df1)

# COMMAND ----------

# DBTITLE 1,Final Department
df_department = df1.withColumn('Department', 
                        when(col('position').rlike('Backend Developer|Frontend Developer|Cloud Engineer|Cyber Security|Data intern| Frontend Developer|Machine Learning|QA|Scrum Master|Software Development TL|Back End Developer|Cloud Engineering|Cybersecurity Analyst|Quality Assurance|Database Engineering|Scrum Master|Linguistics and LocalizationFull Stack Developer|Data Engineer'), 'Software Development')\
                        .when(col('position').rlike('UI/UX|UX Researcher|Product Manager|Product TL|Product Marketing|Product Manager|Accessibility Design Associate|HCI Specialist'), 'Product')\
                        .when(col('position').rlike('Business Analyst|Operations Associate|Technical Business Analyst|Business Development'), 'Business Operations')\
                        .when(col('position').rlike('Business Planning|Strategy Planning|Strategy Artificial Intelligence|Strategy'), 'Business Strategy')\
                        .when(col('position').rlike('ESG & CSR|ESG Content Writer|Sustainability Strategy'), 'ESG')\
                        .when(col('position').rlike('IT Support|Systems Administrator'), 'IT Operations')\
                        .when(col('position').rlike('Talent Acquisition|People and Culture TL|HRIS Analyst|HR Process Analyst'), 'People and Culture')\
                        .when(col('position').rlike('PMO|Internal Strategy|Process Engineering|Data analytics and Visualization|IT|Operations TL|Data Analyst|HR Process Analyst|HRIS Analyst|Data Analytics and Visualisation|Operations MBA'), 'Operations')\
                        .when(col('position').rlike('Accounting|Financial Modeling|Finance TL|Finance|Accounting|Risk and Compliance Analyst|Sustainable Finance|Financial Planning and Analysis Associate|Financial Modelling'), 'Finance')\
                        .when(col('position').rlike('Human Resources|Human Resources Generalist|Recruitment Specialist|Talent Acquisition|Sourcing Specialist|Human Resources Generalist'), 'Human Resources')\
                        .when(col('position').rlike('Communications and Marketing|Social Media and Digital Marketing|Content & Brand Strategy|Event Planner|PR and Communications|Video Editing|Creative Content Digital marketing Analyst|Content and Brand Strategy|Product Marketing|'), 'Marketing')\
                        .when(col('position').rlike('AI Incubator|AI VENTURE LAB|AI Venture Lab'), 'AI Venture Lab')\
                        .when(col('position').rlike('MarCom TL|MarComm'), 'MarCom')\
                        .when(col('position').rlike('AI Tool Trainer|Business Communications Specialist|Data analyst|Executive assistant|Project Management|Thought leadership writer/ social media'), 'COSO')\
                        .when(col('position').rlike('MBA'),'MBA')\
                        .when(col('position').rlike('Marketing'),'Marketing')\
                        .when(col('position').rlike('Capital Market'),'Capital Market')\
                        .when(col('position').rlike('Sustainability Strategy|Strategy MBA Internship|Capital Markets|Client Acquistion'),'BD/Strategy'))

display(df_department)

# COMMAND ----------

# DBTITLE 1,Source
df_source = df_department.withColumnRenamed('source', 'website')\
         .withColumn('source', when(col('website').isin(["vosyn.ai","vosyn.com"]), 'Company Website')\
                              .when(col('website').isin(["Indeed","Monster","eluta.ca","builtin", "Simplify","aiapply.co","Jobgether","duckduckgo.com","ziprecruiter","remotive"]), 'Job Board')\
                              .when(col('website').isin(["LinkedIn","t.co"]), 'Social Media')\
                              .when(col('website').isin(["Career Portal"]), 'Career Portal')\
                              .when(col('website').isin(["google_jobs_apply","www.google.com","Google","api.cactus-search.com","www.eluta.ca","cac-excel.officeapps.live.com","haitou.zhitongguigu.com","neuvoo","www.uxrhunt.com","chatgpt.com","ca.search.yahoo.com","www.ntnu.no","kubecareers","app.joinwayble.com","kth-exjobb.powerappsportals.com"]), 'Search Engine')\
                              .when(col('website').isin(["app.joinhandshake.com","joinhandshake.com",".12twenty.com","symplicity.com","targetconnect.net","jobgether.com","ca.prosple.com"]), 'University Careers Page')\
                              .when(col('website').rlike("joinhandshake|app.joinhandshake.com|joinhandshake.com|.12twenty.com|symplicity.com|targetconnect.net|ccr.trentu.ca|experience.yorku.ca|mycareer.bristol.ac.uk|macarriere.hec.ca|experienceguelph.ca|success.uwindsor.ca"), 'University Careers Page'))
display(df_source)
          


# COMMAND ----------

# DBTITLE 1,Stage
df_stage = df_source.withColumn('stage',
                 when(col('stage')=='Application Received', 'Application Received')\
                .when(col('stage').isin(['Reminder WQ', 'Reminder WQ (7 days +)', 'Written Q received']), 'Written Response')\
                .when(col('stage').isin(['Reminder VQ', 'Reminder VQ (7 days +)', 'Video Q Received','WQ + VQ received']), 'Video Response')\
                .when(col('stage')=='Group Interview', 'Group Interview')\
                .when(col('stage')=='Send Contract', 'Send Contract')\
                .when(col('stage')=='Onboarding', 'Onboarding')\
                .when(col('stage').isin(['Hired', 'Hired - Attended KO']), 'Hired')\
                .when(col('stage')=='Rejected', 'Rejected')\
                .otherwise(col('stage')))
display(df_stage)


# COMMAND ----------

# DBTITLE 1,Institute, University
df_institute = df_stage.withColumn('Institute',
                 when(col('website').isin(["hec.joinhandshake.fr", "macarriere.hec.ca"]), 'HEC Montreal')\
                .when(col('website') == 'mycareer.bristol.ac.uk', 'University of Bristol')\
                .when(col('website') == 'ischool-utoronto-csm.symplicity.com','University of Toronto')\
                .when(col('website') == 'iit.joinhandshake.com','Illinois Institute of Technology')\
                .when(col('website') == 'nait-csm.symplicity.com', 'Northern Alberta Institute Of Technology')\
                .when(col('website') == 'experience.yorku.ca','York University')\
                .when(col('website') == 'asu.joinhandshake.com','Arizona State University')\
                .when(col('website') == 'ccr.trentu.ca', 'Trent University')\
                .when(col('website') == 'experienceguelph.ca', 'Guelph University')\
                .when(col('website') == 'success.uwindsor.ca', 'Windsor University')\
                .when(col('website') == 'sjsu.joinhandshake.com', 'San Jose State University')\
                .when(col('website') == 'fitchburgstate.joinhandshake.com', 'fitchburgstate State University')\
                .when(col('website') == 'buffalo.joinhandshake.com', 'Buffalo State University')\
                .when(col('website') == 'stonybrook.joinhandshake.com', 'Stonybrook University')\
                .when(col('website') == 'utaustin.joinhandshake.com', 'UT Austin')   
                .otherwise(col('website')))\
                .withColumn('University', when(col('source') == 'University Careers Page', 'University')\
                                         .otherwise(lit('')))
display(df_institute)


# COMMAND ----------

# DBTITLE 1,date month
df_date = df_institute.withColumn("Month Number", month('addedDate'))\
                      .withColumn("Month", date_format('addedDate','MMMM'))\
                      .withColumn("Year", year('addedDate'))
display(df_date)

# COMMAND ----------

# DBTITLE 1,AI venture
df_ai_venture = df_date.withColumn('AI Venture Lab (Positions)', 
                          when(col('position').rlike("AI"), col('position'))\
                         .otherwise(lit('')))\
                          .withColumn('AI Venture Lab',
                            when(col('AI Venture Lab (Positions)').rlike("AI"), 
                                 lit('AI Venture Lab'))
                        .otherwise(lit('')))
display(df_ai_venture)

# COMMAND ----------

df_social_media = df_ai_venture.withColumn(
    'Social Media',
    when(col('Institute').isin(["LinkedIn", "t.co", "Instagram"]), col('Institute'))
    .otherwise(lit(''))  # Use an empty string when the condition is not met
)

display(df_social_media)                                

# COMMAND ----------

df_social_media = df_ai_venture.withColumn(
    'Social Media',
    when(col('Institute').isin(["LinkedIn", "t.co", "Instagram"]), col('Institute'))
    .otherwise(lit(''))  # Use an empty string when the condition is not met
)

display(df_social_media)                                

# COMMAND ----------

df_campus_recruitment_platform = df_social_media.withColumn(
    'Campus Recruitment Platform',
    when(
        col('website').rlike("joinhandshake") |
        col('website').rlike(".12twenty.com") |
        col('website').rlike("symplicity.com") |
        col('website').rlike("targetconnect.net"),
        col('website')
    )
    .otherwise(lit(''))  # Use an empty string when the condition is not met
)

display(df_campus_recruitment_platform)  



# COMMAND ----------

df_rejected = df_campus_recruitment_platform.withColumn(
    'Rejected',when(col('stage').isin("Rejected"), col('stage'))
    .otherwise(lit(''))  # Use an empty string when the condition is not met
)

display(df_rejected)  


# COMMAND ----------

Total_applications = df_ai_venture.count()
monthly_counts = df_ai_venture.groupBy("Month","Month Number").agg(count("name").alias("COUNT"))\
    .withColumn('Total_applications',lit(Total_applications))\
    .orderBy("Month Number") 


display(monthly_counts)


# COMMAND ----------

AI_Venture_Counts = df_ai_venture.filter(col("AI Venture Lab (Positions)")!='').groupBy("AI Venture Lab (Positions)").agg(count("name").alias("COUNT"))\
    #.withColumn('Total_applications',lit(Total_applications))\


display(AI_Venture_Counts)

# COMMAND ----------

# Department = count(Department)
# Source = count(source)
dep_df = df_ai_venture.groupBy("Department").agg(count("Department").alias("Dept_Count"))
display(dep_df)

pivotDF = df_ai_venture.groupBy("Department").pivot("source").agg(count("Department").alias("Dept_source_Count"))
display(pivotDF)

join_df = dep_df.join(pivotDF,dep_df["Department"]== pivotDF["Department"])
display(join_df)

