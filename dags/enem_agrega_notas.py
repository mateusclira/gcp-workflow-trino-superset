from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean
   
# set conf
conf = (
SparkConf()
    .set("google.cloud.auth.service.account.enable", "true")
    .set("google.cloud.auth.service.account.email", "marina.marinalira@gmail.com")
    .set("spark.hadoop.fs.gs.project.id", "igti-edc-mod4")
    .set("spark.hadoop.fs.gs.auth.service.account.private.key", "<-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDlol3mNO0MNEh7\nQc7jwvW2Qfv1QdxTYHf/t8l5OjJYox0KeSxyTT8oRwzrJ436tCUD1fTSRUB0yRkn\nAukb8uvJuJSypNwkh+HZ0snILi8zUbC90dNa2oGdh3rAjGVyVZbAtT1LmU5EYXy6\n6h6GuwjlLm3zXIYxjd9fhc3NU9ZbPdpF3IrI5H4YNIuV0GKPYefWqf17Pw5Sr/eJ\nFo+puYJfD8SoIynWSxpCUWcmIutoYRM0KKo78aqTcS4qgM8/2GlqcDtEm+Qlj3D1\n2LjVfQ02o9FXyK7Tr655HycOi55jlJ3uTd7E/zOeH8ueYn9jRtqxM5ImRPBhQ+2f\n3nk5Ce03AgMBAAECggEADcPYXhrRFNizeZP9y2Bd60F0UYTTqLnRJ6aEMLyg1Phe\nHskZtXZU8Vyk9RmnZJ5U07CJHuugy/9b/x8pCxBaOvrSCw1f1t7AVpbQmqvOD4T3\nk8FWqo0LlP5QiOdBk4N26HFUzcnQS8AOQoQTNV9Tzq0kUSC8OI85ExhJuGOsp0Zm\nU/m2+DpkxGaN7ysxLneLIul88rZgc9LdYWGwk0obsMbNDeritgJNWShXt1bC1/nQ\nMpjntNdLR5BIz/G31gIRQfJ93TH+hcnGtpmrFR2u2t9JQgQ8rwgxWThK/COBC80t\nQR9n3x5dWaZ+PMM1V8VJer+P3iwdfAEAh6bmNhC6PQKBgQD0hsECpN4mxTqZmdl+\n0K2DlxeWUJkFh7I2OsmlLZIVPxw72rv2S9tgOK9eCGu6RfemHW/N0eUG5siQllxM\nPsUNKhBs81mcm00iqOcP2RPwbYjfEW/V21rZf/HwHd+3vV1PhdtMNoHK3bA8D31A\nFoCwX2puDD/ufnWPqXJRKEgEwwKBgQDwaLqR86YXzUUO99F2UqWPUz5ZeAsPyPuo\nEi5r763lKnnZoAUOA0BLyAg45xONGWBTB1YmKjAUxvsnpeXnFScWhi6eaLo70xgD\niC/AEYG2zIy/jozNrdLNpeMC2GX3kn05eHe4i81/sFFU5KfrmNh119wQV2fq16nw\nDVUhMWBefQKBgQDooqNR7qRjYlSj+ZjQa5wIi2CnYzqR5k2IBln9SeOnTwl/cFRO\nuFCXtJYy8fP7ncFEStEpkT1BWaMK2+Inr7gtFA3YeZSRx7aU+PgLPc0Yej0zbBVn\n8m72aiE0/lbvKbtygrjmQx0sccjO+zWAGWiJY92PoRvZvHoAGYdm6tnJnQKBgHyo\n4a2S2NpYv2l+iWfAfUbvlFISVDIQNqY1H0sq0lIVj2+QcW6NswZYqCLRd6rfsLn9\nAOfzUlFDyLILbUZBCq68T2omxgMFd9tE//WpXkiqkoU15r80Vi36ydcM3PreQ/7L\n/9B69ckzZUkO9B/PdtbMYrk6iZEV2XG3iCAZGW6FAoGAOX1eFe7Aig0hLHMF4pp6\nKPLmWZw8rGv9XRI/xfkpzIATH7NkHe3ry1l8b8mQLssRXtOSGiTV7BgfYg8CM3o/\nMfWYaDg85ktI/gptMUO86S06EBnCyELJbDxsfpnfABXeYo+BM/SPhfld+IlZUmjw\n8yPYIZhggqU6s2o4+kiSvDc=\n-----END PRIVATE KEY-----\n>")
    .set("spark.hadoop.fs.gs.auth.service.account.private.key.id", "6a066667ff752dfad16e253eaa6dfa2f0cd3950a")
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
        .groupBy("SG_UF_RESIDENCIA")
        .agg(mean(col("NU_NOTA_MT")).alias("med_mt"))
    )

    uf_cn = (
        df
        .groupBy("SG_UF_RESIDENCIA")
        .agg(mean(col("NU_NOTA_CN")).alias("med_cn"))
    )

    uf_ch = (
        df
        .groupBy("SG_UF_RESIDENCIA")
        .agg(mean(col("NU_NOTA_CH")).alias("med_ch"))
    )

    uf_lc = (
        df
        .groupBy("SG_UF_RESIDENCIA")
        .agg(mean(col("NU_NOTA_LC")).alias("med_lc"))
    )

    uf_notas = (
        uf_mt
        .join(uf_cn, on="SG_UF_RESIDENCIA", how="inner")
        .join(uf_ch, on="SG_UF_RESIDENCIA", how="inner")
        .join(uf_lc, on="SG_UF_RESIDENCIA", how="inner")
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
    