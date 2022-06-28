from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, substring

conf = (
SparkConf()
    .set("google.cloud.auth.service.account.enable", "true")
    .set("google.cloud.auth.service.account.email", "mateusc.lira@gmail.com")
    .set("spark.hadoop.fs.gs.project.id", "igti-edc-mod4")
)

if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
            .builder\
            .appName("ENEM Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark
        .read
        .format("parquet")
        .load("gs://mateus-processing-zone/enem/")
    )
    
    print("*************")
    print("* ANONIMIZA *")
    print("*************")

    inscricao_oculta = (
        df
        .withColumn("inscricao_string", df.NU_INSCRICAO.cast("string"))
        .withColumn("inscricao_menor", substring(col("inscricao_string"), 5, 4))
        .withColumn("inscricao_oculta", concat(lit("*****"), col("inscricao_menor"), lit("***")))
        .select("NU_INSCRICAO", "inscricao_oculta", "NU_NOTA_MT", "NO_MUNICIPIO_PROVA")
    )

    (
        inscricao_oculta
        .write
        .mode("overwrite")
        .format("parquet")
        .save("gs://mateus-serving-zone/enem_anon/")
    )

    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()