import org.apache.hadoop.shaded.com.squareup.okhttp.internal.io.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

import java.nio.file.FileSystems


object solution {
  println(FileSystems.getDefault().getPath(""))
  // Путь к папке resources в корне проекта
  val DATASET_PATH = "resources"

  // Схемы для чтения csv таблиц
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

  def main(args: Array[String]) = {
    // Создаем сессию спарка
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("solution")
      .getOrCreate()

    // Читаем csv файлы
    var customer = spark
      .read
      .option("delimiter", "\t")
      .option("header", "false")
      .schema(customerSchema)
      .csv(s"$DATASET_PATH/customer.csv")

    var order = spark
      .read
      .option("delimiter", "\t")
      .option("header", "false")
      .schema(orderSchema)
      .csv(s"$DATASET_PATH/order.csv")

    var product = spark
      .read
      .option("delimiter", "\t")
      .option("header", "false")
      .schema(productSchema)
      .csv(s"$DATASET_PATH/product.csv")

    // Фильтруем датафреймы, это опционально, так как в задаче это не сказано
    // Но в данных явно есть выбросы
    customer = customer.filter(customer("status") === "active")
    order = order.filter(order("status") === "delivered")

    // Расчитаем датафрейм с кол-вом продуктов, которые купил пользователь
    val sumProducts = order
      .select("customerID", "productID", "numberOfProducts")
      .groupBy("customerID", "productID").sum("numberOfProducts")

    // Расчитаем датафрейм с максимальным кол-во продуктов, которые купил пользователь
    val maxSumProducts = sumProducts
      .groupBy("customerID")
      .max("sum(numberOfProducts)")
      .select("customerID", "max(sum(numberOfProducts))")
      .withColumnRenamed("max(sum(numberOfProducts))", "maxProducts")

    // Получаем датафрейм с популярными продуктами у пользователя
    val resultIds = sumProducts
      .join(maxSumProducts, "customerID")
      .filter(col("sum(numberOfProducts)") === col("maxProducts"))

    // Добавляем по id информацию о пользователях и продуктах для ответа
    val result = resultIds
      .join(customer.as("c"), resultIds("customerID") === customer("id"))
      .join(product.as("p"), resultIds("productID") === product("id"))
      .select(col("c.name").as("customer.name"),
              col("p.name").as("product.name")
      )

    // Записываем ответ
    result
      .write
      .option("header", true)
      .mode("overwrite")
      .csv(s"$DATASET_PATH/result.csv")
  }
}
