import org.apache.spark.sql.functions._

val overdose = spark.read.option("header","true").option("inferSchema","true")
  .csv("file:///mnt/c/Users/karki/Downloads/vdh-pud-overdose-ed-visits-by-year-and-geography (1).csv")

val overdoseClean = overdose
  .filter(col("Overdose ED Visit Drug Type") === "All Drug")
  .select(
    col("Overdose ED Visit Year").as("year"),
    col("Overdose ED Visit Patient Geography Name").as("locality"),
    col("Overdose ED Visit Count").as("od_count"),
    col("Overdose ED Visit Rate per 10,000 visits").as("od_rate")
  ).na.drop()

val acsRaw = spark.read.option("header","true").option("inferSchema","true")
  .csv("file:///mnt/c/Users/karki/Downloads/ACSDT1Y2024.B17018-Data.csv")

val acsClean = acsRaw.filter(col("GEO_ID") =!= "Geography")
  .filter(col("NAME").contains("Virginia"))
  .withColumn("county", split(col("NAME"), ",")(0))

val acsFinal = acsClean
  .withColumn("total_families", col("B17018_001E").cast("double"))
  .withColumn("below_poverty", col("B17018_002E").cast("double"))
  .withColumn("poverty_rate", col("below_poverty") / col("total_families"))
  .select("county", "poverty_rate")
  .na.drop()

val overdoseClean2 = overdoseClean
  .withColumnRenamed("locality", "county")
  .withColumn("county", lower(trim(regexp_replace(regexp_replace(col("county"), " County| city| City", ""), "\"", ""))))

val acsFinal2 = acsFinal
  .withColumn("county", lower(trim(regexp_replace(regexp_replace(col("county"), " County| city| City", ""), "\"", ""))))

val joined = overdoseClean2.join(acsFinal2, Seq("county"), "inner")

val finalAnalysis = joined.withColumn("poverty_level",
  when(col("poverty_rate") < 0.05, "Low")
  .when(col("poverty_rate") < 0.10, "Medium")
  .otherwise("High")
)

val result = finalAnalysis.groupBy("poverty_level")
  .agg(avg("od_rate").as("avg_overdose_rate"), count("*").as("records"))

result.show(false)