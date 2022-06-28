from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

# set conf
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
    
    print("****************")
    print("* AGREGA SEXO *")
    print("****************")

    uf_m = (
        df
        .where("TP_SEXO = 'M'")
        .groupBy("NO_MUNICIPIO_PROVA")
        .agg(count(col("TP_SEXO")).alias("count_m"))
    )

    uf_f = (
        df
        .where("TP_SEXO = 'F'")
        .groupBy("NO_MUNICIPIO_PROVA")
        .agg(count(col("TP_SEXO")).alias("count_f"))
    )

    uf_sexo = uf_m.join(uf_f, on="NO_MUNICIPIO_PROVA", how="inner")

    (
        uf_sexo
        .write
        .mode("overwrite")
        .format("parquet")
        .save("gs://mateus-processing-zone/intermediarias/uf_sexo/")
    )

    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()
    