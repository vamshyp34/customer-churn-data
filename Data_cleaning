spark.conf.set("fs.azure.account.auth.type.customerchurndata.dfs.core.windows.net", "SAS") 
spark.conf.set("fs.azure.sas.token.provider.type.customerchurndata.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider") 
spark.conf.set("fs.azure.sas.fixed.token.customerchurndata.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-07-14T03:05:55Z&st=2024-06-13T19:05:55Z&spr=https&sig=dApFDc8OKojyw1RinZMixbM8v6U%2FFV5ARHIfMCFLH7c%3D")

#import necessary modules
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *

churn1 = spark.read.format("csv").option("header","true").load("abfs://churn-container@customerchurndata.dfs.core.windows.net/raw-data/churn1.csv")

churn2 = spark.read.format("csv").option("header","true").load("abfs://churn-container@customerchurndata.dfs.core.windows.net/raw-data/churn2.csv")

#data cleaning for both datasets
# 1. REMOVING DUPLICATES
churn1 = churn1.dropDuplicates()
churn2 = churn2.dropDuplicates()

#data cleaning for both datasets
# 2. DATA TYPE CONVERSION - example--> age into integer, balance into double
# column "balance" from churn1 and column "age" from churn2 will be transformed into double and integer respectively
churn1 = churn1.withColumn('balance',col('balance').cast('double'))
churn2 = churn2.withColumn('age',col('age').cast('int'))
churn1.printSchema()
churn2.printSchema()

# combing both the dataframes as the column "customer_id" is common
# using broadcast feature while joining 
churn1_count = churn1.count()
churn2_count =churn2.count()
if churn1_count < churn2_count:
    churn = churn2.join(broadcast(churn1), on="customer_id", how = "inner" )
else:
    churn = churn1.join(broadcast(churn2), on ="customer_id", how = "inner")
churn.show()

#data cleaning
# 3. REMOVING IRRELEVANT FEATURES - removing the inactive customers
churn = churn.filter(churn.active_member == 1) 
churn.show()

#data cleaning for both datasets
# 4. HANDLE MISSING VALUES - (imputation, removal, mean, mode)
# in our project, we will do imputation (replace missing values with 0)
chrun = churn.na.fill({'credit_score':0, 'balance': 0, 'estimated_salary':0})

#data cleaning
# 5. HANDLE OUTLIERS - We want only the data of customers having credit score above 450
churn = churn.filter(col('credit_score') > 450)
churn.show()

#data cleaning
#6. DATA INTEGRITY --> age and balance should be above 0
churn = churn.filter((col('age')>0) & (col('balance')> 0))

#loading on the adls
churn.repartition(1).write.mode("overwrite").option('header',True).csv('abfs://churn-container@customerchurndata.dfs.core.windows.net/transformed-data/churn.csv')

