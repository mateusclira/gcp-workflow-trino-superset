from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, substring

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
    
    print("*************")
    print("* ANONIMIZA *")
    print("*************")

    inscricao_oculta = (
        df
        .withColumn("inscricao_string", df.NU_INSCRICAO.cast("string"))
        .withColumn("inscricao_menor", substring(col("inscricao_string"), 5, 4))
        .withColumn("inscricao_oculta", concat(lit("*****"), col("inscricao_menor"), lit("***")))
        .select("NU_INSCRICAO", "inscricao_oculta", "NU_NOTA_MT", "SG_UF_RESIDENCIA")
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
    