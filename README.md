# SparkScala

# Examen Scala con Apache Spark

Este repositorio contiene una práctica en Scala que implementa varios ejercicios utilizando Apache Spark. La práctica abarca la creación y manipulación de DataFrames, definición y aplicación de UDFs, joins y agregaciones, operaciones con RDDs y procesamiento de archivos CSV.

## Contenido del repositorio

- **src/main/scala/job/examen/Examen.scala**: Implementación de las funciones solicitadas en los ejercicios.
- **src/main/scala/job/examen/Main.scala**: Archivo principal que configura una sesión Spark, crea datos de ejemplo y ejecuta cada uno de los ejercicios.
- **src/test/scala/job/examen/ExamenTest.scala**: Tests unitarios utilizando ScalaTest para validar la funcionalidad de cada ejercicio.

## Requisitos

- Java 8 o superior.
- [Apache Spark](https://spark.apache.org/) (compatible con las versiones 2.x o 3.x).
- [Scala](https://www.scala-lang.org/) (versión 2.11 o 2.12, según la configuración de Spark).
- [SBT](https://www.scala-sbt.org/) para compilar y ejecutar el proyecto.

### Dependencias

Incluir en archivo `build.sbt` las siguientes dependencias:

```sbt
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.scalatest" %% "scalatest" % "3.2.10" % Test
)
