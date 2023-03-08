'''Crea un programa Spark que permita leer el fichero de datos: real-estate.csv.

Pregunta a responder:

Agrupa el precio medio por m2 y ordenalo de menor a mayor. **Se trata de poder 
indicar poblaciones más caras.** Guarda el resultado en único archivo.'''

# Importamos los módulos que vamos a necesitar
import os
import sys
sys.path.insert(0, ".")

from commons.utils import FolderConfig

from pyspark.sql import SparkSession
from pyspark.sql.functions import round
from pyspark.sql.types import *

# Declaramos el esquema que vamos a usar
real_estate_schema = StructType(
    [
        StructField("MLS", IntegerType(), True),
        StructField("Location", StringType(), True),
        StructField("Price", DoubleType(), True),
        StructField("Bedrooms", IntegerType(), True),
        StructField("Bathrooms", IntegerType(), True),
        StructField("Size", DoubleType(), True),
        StructField("Price SQ M2", DoubleType(), True),
        StructField("Status", StringType(), True)
    ]
)

if __name__ == "__main__":

    fc = FolderConfig(file=__file__).clean()
    DIRECTORY_IN = str(fc.input)
    DIRECTORY_OUT = str(fc.output)

    # 1 - Instanciamos la sesión de spark
    spark = (
        SparkSession
        .builder
        .appName("Real Estate Project")
        .getOrCreate()
    )

    # Hacemos que sólo aparezcan errores y no warnings
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    # 2 - Leemos los datos
    # 2.1 Obtenemos el fichero real-state.csv del input
    real_estate_path = str(os.path.join(DIRECTORY_IN, "real-estate.csv"))

    # 2.2 Leemos el fichero con un Spark DataFrame

    real_estate_df = (
        spark.read.csv(real_estate_path,                                # Cargamos en formato CSV
        header = True,                                                  # Sí tiene headers
        schema = real_estate_schema)                                    # Decimos el esquema
    )
    
    print('\n'*4)
    print('--- REAL ESTATE ORIGINAL TABLE ---')
    real_estate_df.show(n=10, truncate=False)

    # 3 - Agregamos el precio por metro cuadrado por localización según la media
    #     y ordenamos de menor a mayor
    pricing_real_estate_df = (
        real_estate_df
        .select("Location", "Price", "Size", "Price SQ M2")             # Transformación
        .groupBy("Location")                                            # Transformación
        .mean("Price", "Size", "Price SQ M2")                           # Acción (aggregate function)                                       
        .orderBy("avg(Price SQ M2)", ascending=True)                    # Transformación
        .withColumn("avg(Price)", round("avg(Price)", 2))               # Transformación
        .withColumn("avg(Size)", round("avg(Size)", 2))                 # Transformación
        .withColumn("avg(Price SQ M2)", round("avg(Price SQ M2)", 2))   # Transformación
    )

    # 4 - Mostramos los resultados de la agregación
    print('\n--- REAL ESTATE AGGREGATED TABLE ---')
    pricing_real_estate_df.show(n=60, truncate=False)                   # Acción (show)

    cheapest_location = (
    pricing_real_estate_df
    .orderBy("avg(Price SQ M2)", ascending=True)
    .first()
    )

    most_expensive_location = (
    pricing_real_estate_df
    .orderBy("avg(Price SQ M2)", ascending=False)
    .first()
    )

    print('\n--- CONCLUSIONS DRAWN... ---')
    print("Location with the cheapest price per sq m2:")
    print(cheapest_location)
    print("Location with the most expensive price per sq m2:")
    print(most_expensive_location)
    print("\n")

    # 5 - Exportamos los resultados a un archivo CSV usando coalesce 
    # para forzar que la escritura se haga en un mismo fichero
    pricing_real_estate_df.coalesce(1).write.csv(os.path.join(DIRECTORY_OUT, 'real-estate-grouped.csv'))