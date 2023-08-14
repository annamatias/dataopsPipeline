import logging
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def setup_session():
    builder = SparkSession.builder.appName("Ingestão Cardio") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    return configure_spark_with_delta_pip(builder).getOrCreate()    


def read_csv(spark, path="/Users/annakarolinymatias/Documents/dataopsPipeline/data_sources/cardiovasculas.csv"):
    logging.info("Realizando leitura do arquivo")
    return spark.read.format("csv").option("header", "true").load(path)


def rename_columns(df):
    logging.info("Renomeando colunas")
    return df.withColumnRenamed("Height_(cm)", "Height_cm").withColumnRenamed("Weight_(kg)", "Weight_kg")


def save_delta(df):
    logging.info("Armazenando dados")
    return df.write.format("delta").mode("overwrite").option("mergeSchema", True).partitionBy("General_Health").save("/Users/annakarolinymatias/Documents/dataopsPipeline/storage/hospital/rw/cardiovascular/")


def main():
    spark = setup_session()
    df = read_csv(spark)
    df = rename_columns(df)
    save_delta(df)
    spark.stop()


if __name__ == '__main__':
    main()