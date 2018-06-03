package Hospital_Data_Analysis

import org.apache.spark.sql.SparkSession

object Hospital_Objective1 {

  def main(args: Array[String]): Unit = {


    println("Hospital data analysis in US")

    //Create the spark session
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Hospital Data Analysis in US")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    //Use CSV load method  to load the data and use infer schema  as an option so it will automatically infer the data type of the columns
    val in_patient_charges = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\Bhaskar\\Desktop\\AcadGild\\CaseStudies\\CaseStudy4 Hospital\\inpatientCharges.csv")

    //Use printSchema to check the schema of the loaded file
    in_patient_charges.printSchema()


    //Save the data in table hospital_charges  by registering it in temp table
    in_patient_charges.registerTempTable("hospital_charges")

    println("inpatientCharges data is registered in temporary table hospital-charges")

  }
}