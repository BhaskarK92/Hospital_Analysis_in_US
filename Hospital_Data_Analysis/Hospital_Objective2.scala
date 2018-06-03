package Hospital_Data_Analysis

import org.apache.spark.sql.SparkSession

object Hospital_Objective2
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

    //Use groupBy sql function to group ProviderState and average function to average AverageCoveredCharges respect to ProviderState
    in_patient_charges.groupBy("ProviderState").avg("AverageCoveredCharges").show()

    //Use groupBy sql function to group ProviderState and average function to average AverageTotalPayments respect to ProviderState
    in_patient_charges.groupBy("ProviderState").avg("AverageTotalPayments").show()

    //Use groupBy sql function to group ProviderState and average function to average AverageMedicarePayments respect to ProviderState
    in_patient_charges.groupBy("ProviderState").avg("AverageMedicarePayments").show()

  }
}
