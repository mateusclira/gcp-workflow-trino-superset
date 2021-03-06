from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

#set conf
conf = (
SparkConf()
    .set("google.cloud.auth.service.account.enable", "true")
    .set("google.cloud.auth.service.account.email", "mateusc.lira@gmail.com")
    .set("spark.hadoop.fs.gs.project.id", "igti-edc-mod4")
)

sc = SparkContext(conf=conf).getOrCreate()
    

if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
            .builder\
            .appName("ENEM Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    uf_sexo = (
        spark
        .read
        .format("parquet")
        .load("gs://mateus-processing-zone/intermediarias/uf_sexo/")
    )

    uf_notas = (
        spark
        .read
        .format("parquet")
        .load("gs://mateus-processing-zone/intermediarias/uf_notas/")
    )
    
    print("****************")
    print("* JOIN FINAL *")
    print("****************")

    uf_final = (
        uf_sexo
        .join(uf_notas, on="NO_MUNICIPIO_PROVA", how="inner")
    )

    (
        uf_final
        .write
        .mode("overwrite")
        .format("parquet")
        .save("gs://mateus-serving-zone/enem_uf/")
    )

    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()
    