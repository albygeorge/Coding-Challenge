# Coding-Challenge
Assignment Solution

 
Note: As of now I am taking it as an Integer due to facing conversion issues with the UUID columns while writing the data to postgres.
create table Department(
id integer,
name Text,
salary_increment numeric
);



create table employee
(
	id integer,
	first_name Text,
	last_name Text,
	salary numeric,
	department_id integer
);

AWS Glue Code:

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "flat_employee_db", table_name = "flat_data1_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
empinfoDF = glueContext.create_dynamic_frame.from_catalog(database = "flat_employee_db", table_name = "flat_data1_csv").toDF()

empDF = empinfoDF.withColumnRenamed('employee_id','id').select('id','first_name','last_name','salary','department_id')
deptDF = empinfoDF.withColumnRenamed('department_id','id').withColumnRenamed('department_name','name').select('id','name','salary_increment')

dynamic_emp_frame_write = DynamicFrame.fromDF(empDF,glueContext,'dynamic_emp_frame_write')
dynamic_dept_frame_write = DynamicFrame.fromDF(deptDF,glueContext,'dynamic_dept_frame_write')

## @type: DataSink
## @args: [catalog_connection = "PostgresConnection", connection_options = {"database" : "postgres", "dbtable" : "Employee"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: [frame = <frame>]
empResult = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dynamic_emp_frame_write, catalog_connection = "PostgresConnection", connection_options = {"database" : "postgres", "dbtable" : "Employee"}, redshift_tmp_dir = args["TempDir"])
deptResult = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dynamic_dept_frame_write, catalog_connection = "PostgresConnection", connection_options = {"database" : "postgres", "dbtable" : "Department"}, redshift_tmp_dir = args["TempDir"])

resultDF = deptDF.join(empDF,deptDF.id == empDF.department_id).drop(deptDF.id).withColumnRenamed('id','employee_id').select('employee_id','salary','salary_increment')
finalDF = resultDF.withColumn('updated_salary',resultDF['salary']+(resultDF['salary']*resultDF['salary_increment']/100)).select('employee_id','updated_salary')
dynamic_result_frame_write = DynamicFrame.fromDF(finalDF,glueContext,'dynamic_result_frame_write')
finalResult = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dynamic_result_frame_write, catalog_connection = "PostgresConnection", connection_options = {"database" : "postgres", "dbtable" : "dynamic_result_frame_write"}, redshift_tmp_dir = args["TempDir"])
job.commit()

PySpark Version of Code:
 

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
import uuid

if __name__ == "__main__":
    spark = SparkSession    \
    .builder    \
    .master("local[2]") \
    .config('spark.jars', 'lib/postgresql-42.2.18.jar') \
    .appName("EmpDeptJob")  \
    .getOrCreate()

    empInfoSchemaStruct = StructType([
        StructField("employee_id",IntegerType()),
        StructField("first_name",StringType()),
        StructField("last_name",StringType()),
        StructField("salary",StringType()),
        StructField("department_id", IntegerType()),
        StructField("department_name",StringType()),
        StructField("salary_increment",IntegerType())
    ])


    empinfoDF = spark.read.format('csv').option("header","true").schema(empInfoSchemaStruct).load("dataSource/flat_data1.csv")
    print(empinfoDF.printSchema())

    empDF = empinfoDF.withColumnRenamed('employee_id','id').select('id','first_name','last_name','salary','department_id')
    deptDF = empinfoDF.withColumnRenamed('department_id','id').withColumnRenamed('department_name','name').select('id','name','salary_increment')
    print(empDF.show())
    print(deptDF.show())

    empDF.write.format("jdbc").options(url='jdbc:postgresql://localhost:5432/northwind',dbtable='public.Employee',user='postgres',password='postgres',driver='org.postgresql.Driver').mode('overwrite').save()
    deptDF.write.format("jdbc").options(url='jdbc:postgresql://localhost:5432/northwind',dbtable='public.Department',user='postgres',password='postgres',driver='org.postgresql.Driver').mode('overwrite').save()
    print(empDF.printSchema())
    empDF.withColumnRenamed('id','employee_id')
    resultDF = deptDF.join(empDF,deptDF.id == empDF.department_id).drop(deptDF.id).withColumnRenamed('id','employee_id').select('employee_id','salary','salary_increment')
    print(resultDF.show())
    finalDF = resultDF.withColumn('updated_salary',resultDF['salary']+(resultDF['salary']*resultDF['salary_increment']/100)).select('employee_id','updated_salary')

    finalDF.write.format("jdbc").options(url='jdbc:postgresql://localhost:5432/northwind',dbtable='public.updated_salaries',user='postgres',password='postgres',driver='org.postgresql.Driver').mode('overwrite').save()


Note: As of now we have hardcoded the database connection information within the python code but will make it as parameterized while deploying the application by keeping the secrets in the secure place.

