import org.apache.spark.sql._

object CsvDAO{
    def readData(spark: SparkSession, gcsPath: String): DataFrame={
        spark.read.format("csv").option("header", true).load(gcsPath)
    }

    def writeData(spark: SparkSession, df : DataFrame, gcsPath: String){
        df.write.format("csv").save(gcsPath)
    }
}