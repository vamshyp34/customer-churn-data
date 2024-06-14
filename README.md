# customer-churn-data

Raw data is stored in github

Created Resource group

Created Storage Acount

Used the Raw URL to extract data from GitHub

Created Azure Data Factory

Defined linked services, dataset and created pipeline with copy activity

Pipeline ran successfully and files stored in raw folder in ADLS GEN2 container after copy activities

Created Azure Databricks workspace 

Created Cluster and then the Notebook

Mounted ADLS raw folder to Databricks using SAS key

Read files on to Databricks Notebook 

performed data cleaning #Refer Data-Cleaning.py file attached

Loaded the cleaned data on to ADLS container in transformed folder 

Created Synapse Analytics Workspace 

Pulled data from transformed container in ADLS container on to Synapse

Performed queries using SQL #Refer to synapse_sql.sql file attached

Connected data from Synapse Analytics to Power BI

Created Dasboard 



   
