from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

# set conf
conf = (
SparkConf()
    .set("google.cloud.auth.service.account.enable", "true")
    .set("google.cloud.auth.service.account.email", "s@gmail.com")
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
    print("* AGREGA IDADE *")
    print("****************")

    uf_idade = (
        df
        .groupBy("SG_UF_RESIDENCIA")
        .agg(mean(df.NU_IDADE).alias("med_nu_idade"))
    )

    (
        uf_idade
        .write
        .mode("overwrite")
        .format("parquet")
        .save("gs://mateus-processing-zone/intermediarias/uf_idade/")
    )

    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()
    