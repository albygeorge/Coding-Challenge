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