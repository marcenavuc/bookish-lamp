import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

val spark = SparkSession
  .builder()
  .master("local[1]")
  .appName("solution")
  .getOrCreate()

// Define schemas
val customerSchema = StructType(Array(
  StructField("id", IntegerType),
  StructField("name", StringType),
  StructField("email", StringType),
  StructField("joinDate", DateType),
  StructField("status", StringType),
))

val productSchema = StructType(Array(
  StructField("id", IntegerType),
  StructField("name", StringType),
  StructField("price", DoubleType),
  StructField("numberOfProducts", IntegerType),
))

val orderSchema = StructType(Array(
  StructField("customerID", IntegerType),
  StructField("orderID", IntegerType),
  StructField("productID", IntegerType),
  StructField("numberOfProducts", IntegerType),
  StructField("orderDate", DateType),
  StructField("status", StringType),
))

// Reading csv
var customer = spark
  .read
  .option("delimiter", "\t")
  .option("header", "false")
  .schema(customerSchema)
  .csv("C:\\Users\\marka\\IdeaProjects\\test_task\\resources\\customer.csv")

var order = spark
  .read
  .option("delimiter", "\t")
  .option("header", "false")
  .schema(orderSchema)
  .csv("C:\\Users\\marka\\IdeaProjects\\test_task\\resources\\order.csv")

var product = spark
  .read
  .option("delimiter", "\t")
  .option("header", "false")
  .schema(productSchema)
  .csv("C:\\Users\\marka\\IdeaProjects\\test_task\\resources\\product.csv")

// Filter
customer = customer.filter(customer("status") == "active")
order = order.filter(order("status") == "delivered")

