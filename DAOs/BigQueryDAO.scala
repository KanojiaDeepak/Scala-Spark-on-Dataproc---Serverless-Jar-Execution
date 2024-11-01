import org.apache.spark.sql._

object BigQueryDAO{
    def readData(spark: SparkSession, tablePath: String): DataFrame={
        spark.read.format("bigquery").load(tablePath)
    }

    def writeData(spark: SparkSession, df : DataFrame, tablePath: String, temp_bq_bucket: String){
        df.write.format("bigquery").mode(SaveMode.Overwrite).option("table", tablePath)
        .option("temporaryGcsBucket",temp_bq_bucket).save()
    }
}