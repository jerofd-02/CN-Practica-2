# monthly_aggregation.py
import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, sum as spark_sum, avg, substring, count
from awsglue.dynamicframe import DynamicFrame

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    args = getResolvedOptions(sys.argv, ['database', 'table', 'output_path'])
    database = args['database']
    table = args['table']
    output_path = args['output_path']

    logger.info(f"Database: {database}, Table: {table}, Output: {output_path}")
    
    sc = SparkContext()
    glueContext = GlueContext(sc)

    # Leer desde Glue Catalog usando GlueContext
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table
    )

    # Convertir a Spark DataFrame
    df = dynamic_frame.toDF()
    df.printSchema()
    logger.info(f"Registros leídos: {df.count()}")

    df = df.withColumn("mes", substring(col("flight_date"), 1, 7))
    
    # Agregación mensual
    monthly_df = df.groupBy("mes") \
        .agg(
            spark_sum(col("cancelled")).alias("vuelos_cancelados"),
            spark_sum((col("cancelled") == 0).cast("int")).alias("vuelos_realizados"),
            count("*").alias("vuelos_totales"),
            (avg(col("cancelled")) * 100).alias("porcentaje_cancelados")
        ) \
        .orderBy("mes")
    
    output_dynamic_frame = DynamicFrame.fromDF(monthly_df, glueContext, "output")
    
    logger.info(f"Registros agregados: {output_dynamic_frame.count()}")
    
    # Escribir usando GlueContext
    glueContext.write_dynamic_frame.from_options(
        frame=output_dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": output_path,
            "partitionKeys": ["mes"]
        },
        format="parquet",
        format_options={"compression": "snappy"}
    )
    
    logger.info(f"Completado. Registros: {monthly_df.count()}")

if __name__ == "__main__":
    main()
