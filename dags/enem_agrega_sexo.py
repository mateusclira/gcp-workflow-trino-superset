from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

# set conf
conf = (
SparkConf()
    .set("google.cloud.auth.service.account.enable", "true")
    .set("google.cloud.auth.service.account.email", "marina.marinalira@gmail.com")
    .set("google.cloud.auth.service.account.keyfile", "C:\\Users\\Micro\\Downloads\\mateusclira.json")
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
        .groupBy("SG_UF_RESIDENCIA")
        .agg(count(col("TP_SEXO")).alias("count_m"))
    )

    uf_f = (
        df
        .where("TP_SEXO = 'F'")
        .groupBy("SG_UF_RESIDENCIA")
        .agg(count(col("TP_SEXO")).alias("count_f"))
    )

    uf_sexo = uf_m.join(uf_f, on="SG_UF_RESIDENCIA", how="inner")

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
    