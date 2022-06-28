from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean
   
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
    print("* AGREGA NOTAS *")
    print("****************")

    uf_mt = (
        df
        .groupBy("NO_MUNICIPIO_PROVA")
        .agg(mean(col("NU_NOTA_MT")).alias("med_mt"))
    )

    uf_cn = (
        df
        .groupBy("NO_MUNICIPIO_PROVA")
        .agg(mean(col("NU_NOTA_CN")).alias("med_cn"))
    )

    uf_ch = (
        df
        .groupBy("NO_MUNICIPIO_PROVA")
        .agg(mean(col("NU_NOTA_CH")).alias("med_ch"))
    )

    uf_lc = (
        df
        .groupBy("NO_MUNICIPIO_PROVA")
        .agg(mean(col("NU_NOTA_LC")).alias("med_lc"))
    )

    uf_notas = (
        uf_mt
        .join(uf_cn, on="NO_MUNICIPIO_PROVA", how="inner")
        .join(uf_ch, on="NO_MUNICIPIO_PROVA", how="inner")
        .join(uf_lc, on="NO_MUNICIPIO_PROVA", how="inner")
    )

    (
        uf_notas
        .write
        .mode("overwrite")
        .format("parquet")
        .save("gs://mateus-processing-zone/intermediarias/uf_notas/")
    )

    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()
    