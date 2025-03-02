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
```
# Examen de Scala con Apache Spark

Este repositorio contiene la implementación de una serie de ejercicios en Scala utilizando Apache Spark. Cada ejercicio aborda un concepto clave del procesamiento de datos con Spark, incluyendo DataFrames, UDFs, joins, RDDs y procesamiento de archivos CSV.

## Requisitos

Antes de ejecutar el código, asegúrate de contar con los siguientes requisitos:

- **Scala** (versión 2.12 o superior)
- **Apache Spark** (versión 3.0 o superior)
- **SBT** (para compilar y ejecutar el proyecto)
- **Java** (JDK 8 o superior)

## Estructura del Proyecto

El proyecto está organizado en los siguientes archivos:

- `src/main/scala/job/examen/Examen.scala`: Contiene la implementación de los ejercicios.
- `src/main/scala/job/examen/Main.scala`: Archivo principal para ejecutar todos los ejercicios.
- `src/test/scala/job/examen/ExamenTest.scala`: Pruebas unitarias para cada ejercicio.

## Ejercicios

### **Ejercicio 1: Operaciones básicas con DataFrames**
- Creación de un DataFrame con información de estudiantes.
- Filtrado de estudiantes con calificación mayor a 8.
- Ordenamiento de estudiantes por calificación en orden descendente.

### **Ejercicio 2: Uso de UDF (User Defined Function)**
- Definición de una función para determinar si un número es par o impar.
- Aplicación de la función a una columna de un DataFrame.

### **Ejercicio 3: Joins y agregaciones**
- Realización de un join entre un DataFrame de estudiantes y un DataFrame de calificaciones.
- Cálculo del promedio de calificaciones por estudiante.

### **Ejercicio 4: Uso de RDDs**
- Creación de un RDD a partir de una lista de palabras.
- Conteo de la cantidad de ocurrencias de cada palabra.

### **Ejercicio 5: Procesamiento de archivos CSV**
- Carga de un archivo CSV que contiene información sobre ventas.
- Cálculo del ingreso total por producto.

**Nota:** Para ejecutar el ejercicio 5 correctamente, es necesario proporcionar la ruta del archivo `ventas.csv`. Por defecto, se asume que el archivo se encuentra en el mismo directorio de ejecución. Si está en otra ubicación, debes modificar la llamada en `Main.scala`:

```scala
val resultEj5 = examen.ejercicio5(spark, "ruta/del/archivo/ventas.csv")
```



