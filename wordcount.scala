import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object SampleProgram {
  
  def main(args: Array[String]){
    val spark = SparkSession.builder()
  .appName("simple name")
  .master("local[*]")
  .getOrCreate()
  
  
val inputdf = spark.read
.format("csv")
.option("path", "D:/sparkdatasets/orders.csv")
.option("header", true) 
.option("inferSchema", true) 
.load()


inputdf.show()

inputdf.printSchema()

inputdf.show()
    
  }
  

  
}