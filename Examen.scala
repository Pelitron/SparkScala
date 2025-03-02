package job.examen

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction

object examen {

  /** Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
    * Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
    *           estudiantes (nombre, edad, calificación).
    *           Realiza las siguientes operaciones:
    *
    *           - Muestra el esquema del DataFrame.
    *           - Filtra los estudiantes con una calificación mayor a 8.
    *           - Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
    */
  def ejercicio1(estudiantes: DataFrame)(spark: SparkSession): DataFrame = {
    // Muestra el esquema (side-effect)
    estudiantes.printSchema()
    // Filtra, ordena y selecciona el nombre
    val df = estudiantes.filter(col("calificacion") > 8)
      .orderBy(desc("calificacion"))
      .select("nombre")
    df
  }

  /** Ejercicio 2: UDF (User Defined Function)
    * Pregunta: Define una función que determine si un número es par o impar.
    *           Aplica esta función a una columna de un DataFrame que contenga una lista de números.
    */
  def ejercicio2(numeros: DataFrame)(spark: SparkSession): DataFrame = {
    // Definir la UDF para determinar par/impar
    val parImpar: UserDefinedFunction = udf((n: Int) => if (n % 2 == 0) "Par" else "Impar")
    // Se asume que el DataFrame tiene una columna llamada "numero"
    numeros.withColumn("tipo", parImpar(col("numero")))
  }

  /** Ejercicio 3: Joins y agregaciones
    * Pregunta: Dado dos DataFrames,
    *           uno con información de estudiantes (id, nombre)
    *           y otro con calificaciones (id_estudiante, asignatura, calificacion),
    *           realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
    */
  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {
    // Realizar el join entre los DataFrames usando el id
    val joined = estudiantes.join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))
    // Calcular el promedio de calificaciones agrupado por id y nombre
    joined.groupBy("id", "nombre")
      .agg(avg("calificacion").alias("promedio"))
  }

  /** Ejercicio 4: Uso de RDDs
    * Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.
    */
  def ejercicio4(palabras: List[String])(spark: SparkSession): RDD[(String, Int)] = {
    val rdd = spark.sparkContext.parallelize(palabras)
    rdd.map(word => (word, 1)).reduceByKey(_ + _)
  }

  /**
    * Ejercicio 5: Procesamiento de archivos
    * Pregunta: Carga un archivo CSV que contenga información sobre
    *           ventas (id_venta, id_producto, cantidad, precio_unitario)
    *           y calcula el ingreso total (cantidad * precio_unitario) por producto.
    */
  def ejercicio5(ventas: DataFrame)(spark: SparkSession): DataFrame = {
    // Calcular el ingreso por fila y luego agrupar por producto para sumar
    ventas.withColumn("ingreso", col("cantidad") * col("precio_unitario"))
      .groupBy("id_producto")
      .agg(sum("ingreso").alias("ingreso_total"))
  }

}