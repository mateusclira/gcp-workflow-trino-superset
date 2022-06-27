from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
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
        .format("csv")
        .options(header='true', inferSchema='true', delimiter=';')
        .load("gs://mateus-landing-zone/enem")
    )
    
    df.printSchema()

    (df
    .write
    .mode("overwrite")
    .format("parquet")
    .save("gs://mateus-processing-zone/enem/")
    )

    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()