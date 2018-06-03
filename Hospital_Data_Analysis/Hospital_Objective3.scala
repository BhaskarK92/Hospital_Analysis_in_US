package Hospital_Data_Analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Hospital_Objective3
{

  def main(args: Array[String]): Unit =
  {
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

    //Use groupBy sql function to group ProviderState and DRGDefinition and sum function to sum up TotalDischarges value respect to ProviderState
    in_patient_charges.groupBy(("ProviderState"),("DRGDefinition")).sum("TotalDischarges").show()

    println("The total number of Discharges per state and for each disease")

    //Use groupBy sql function to group ProviderState and DRGDefinition and sum function to sum up TotalDischarges value respect to ProviderState
    //and orderBy function to show the TotalDischarges in descending order
    in_patient_charges.groupBy(("ProviderState"),("DRGDefinition")).sum("TotalDischarges").orderBy(desc(sum("TotalDischarges").toString)).show()

    println("The output in descending order of totalDischarges")
  }
}

