package job.examen

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

class ExamenTest extends AnyFunSuite {

  // Crear SparkSession para test
  val spark: SparkSession = SparkSession.builder()
    .appName("ExamenTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("ejercicio1 - Filtrado y ordenamiento de estudiantes") {
    // Datos de prueba: (nombre, edad, calificacion)
    val data = Seq(
      ("Juan", 20, 9.5),
      ("Maria", 22, 7.0),
      ("Pedro", 21, 8.5),
      ("Lucia", 23, 8.0)
    )
    val df = data.toDF("nombre", "edad", "calificacion")
    val result = examen.ejercicio1(df)(spark)
    val nombres = result.collect().map(_.getString(0)).toList
    // Se esperan solo los estudiantes con calificacion > 8, ordenados descendentemente: ["Juan", "Pedro"]
    assert(nombres == List("Juan", "Pedro"))
  }

  test("ejercicio2 - UDF para determinar par o impar") {
    // Datos de prueba: DataFrame con columna "numero"
    val data = Seq(1, 2, 3, 4).toDF("numero")
    val result = examen.ejercicio2(data)(spark)
    // Convertir a Map para comprobar los valores
    val tipos = result.collect().map(row => (row.getInt(0), row.getString(1))).toMap
    assert(tipos(1) == "Impar")
    assert(tipos(2) == "Par")
    assert(tipos(3) == "Impar")
    assert(tipos(4) == "Par")
  }

  test("ejercicio3 - Join y promedio de calificaciones") {
    // DataFrame de estudiantes: (id, nombre)
    val estudiantes = Seq(
      (1, "Juan"),
      (2, "Maria"),
      (3, "Pedro")
    ).toDF("id", "nombre")

    // DataFrame de calificaciones: (id_estudiante, asignatura, calificacion)
    val calificaciones = Seq(
      (1, "Matematicas", 9.0),
      (1, "Fisica", 8.0),
      (2, "Matematicas", 7.0),
      (3, "Fisica", 10.0),
      (3, "Quimica", 9.0)
    ).toDF("id_estudiante", "asignatura", "calificacion")

    val result = examen.ejercicio3(estudiantes, calificaciones)
    val resultsMap = result.collect().map(row => (row.getInt(0), (row.getString(1), row.getDouble(2)))).toMap

    // Se espera:
    // - Juan: promedio = (9.0 + 8.0)/2 = 8.5
    // - Maria: promedio = 7.0
    // - Pedro: promedio = (10.0 + 9.0)/2 = 9.5
    assert(math.abs(resultsMap(1)._2 - 8.5) < 0.001)
    assert(math.abs(resultsMap(2)._2 - 7.0) < 0.001)
    assert(math.abs(resultsMap(3)._2 - 9.5) < 0.001)
  }

  test("ejercicio4 - Conteo de ocurrencias de palabras en un RDD") {
    val palabras = List("spark", "scala", "spark", "bigdata", "scala", "spark")
    val result = examen.ejercicio4(palabras)(spark)
    val resultMap = result.collect().toMap
    // Se espera: spark -> 3, scala -> 2, bigdata -> 1
    assert(resultMap("spark") == 3)
    assert(resultMap("scala") == 2)
    assert(resultMap("bigdata") == 1)
  }

  test("ejercicio5 - Ingreso total por producto") {
    // Datos de prueba: (id_venta, id_producto, cantidad, precio_unitario)
    val data = Seq(
      (1, "A", 2, 10.0),
      (2, "B", 3, 15.0),
      (3, "A", 1, 10.0)
    ).toDF("id_venta", "id_producto", "cantidad", "precio_unitario")

    val result = examen.ejercicio5(data)(spark)
    val resultMap = result.collect().map(row => (row.getString(0), row.getDouble(1))).toMap
    // Se espera:
    // - Producto A: ingreso = 2*10.0 + 1*10.0 = 30.0
    // - Producto B: ingreso = 3*15.0 = 45.0
    assert(math.abs(resultMap("A") - 30.0) < 0.001)
    assert(math.abs(resultMap("B") - 45.0) < 0.001)
  }

}