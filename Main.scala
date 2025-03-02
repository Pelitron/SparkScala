package job.examen

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ExamenMain")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Ejercicio 1: DataFrame de estudiantes
    println("Ejercicio 1: DataFrame de estudiantes")
    val estudiantesData = Seq(
      ("Juan", 20, 9.5),
      ("Maria", 22, 7.0),
      ("Pedro", 21, 8.5),
      ("Lucia", 23, 8.0)
    )
    val dfEstudiantes = estudiantesData.toDF("nombre", "edad", "calificacion")
    val resultEj1 = examen.ejercicio1(dfEstudiantes)(spark)
    resultEj1.show()

    // Ejercicio 2: UDF para determinar par o impar
    println("Ejercicio 2: UDF para determinar par o impar")
    val numerosData = Seq(1, 2, 3, 4).toDF("numero")
    val resultEj2 = examen.ejercicio2(numerosData)(spark)
    resultEj2.show()

    // Ejercicio 3: Join y promedio de calificaciones
    println("Ejercicio 3: Join y promedio de calificaciones")
    val estudiantesJoin = Seq(
      (1, "Juan"),
      (2, "Maria"),
      (3, "Pedro")
    ).toDF("id", "nombre")
    val calificacionesJoin = Seq(
      (1, "Matematicas", 9.0),
      (1, "Fisica", 8.0),
      (2, "Matematicas", 7.0),
      (3, "Fisica", 10.0),
      (3, "Quimica", 9.0)
    ).toDF("id_estudiante", "asignatura", "calificacion")
    val resultEj3 = examen.ejercicio3(estudiantesJoin, calificacionesJoin)
    resultEj3.show()

    // Ejercicio 4: Conteo de palabras en un RDD
    println("Ejercicio 4: Conteo de palabras en un RDD")
    val palabras = List("spark", "scala", "spark", "bigdata", "scala", "spark")
    val resultEj4 = examen.ejercicio4(palabras)(spark)
    resultEj4.collect().foreach { case (palabra, count) =>
      println(s"$palabra -> $count")
    }

    // Ejercicio 5: Ingreso total por producto
    println("Ejercicio 5: Ingreso total por producto")
    val ventasData = Seq(
      (1, "A", 2, 10.0),
      (2, "B", 3, 15.0),
      (3, "A", 1, 10.0)
    ).toDF("id_venta", "id_producto", "cantidad", "precio_unitario")
    val resultEj5 = examen.ejercicio5(ventasData)(spark)
    resultEj5.show()

    spark.stop()
  }
}