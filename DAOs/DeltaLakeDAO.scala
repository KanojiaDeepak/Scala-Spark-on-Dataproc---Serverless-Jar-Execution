import org.apache.spark.sql._
import io.delta.tables._

object DeltaLakeDAO{
    def readData(spark: SparkSession, gcsPath: String): DataFrame={
        spark.read.format("delta").load(gcsPath)
    }

    def writeData(spark: SparkSession, df : DataFrame, gcsPath: String){
        df.write.format("delta").mode("overwrite").save(gcsPath)
    }
}