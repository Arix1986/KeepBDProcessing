package keepcod

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import scala.util.matching.Regex
object App {

  case class Estudiante(nombre:String,edad:Integer,sexo:String,calificacion:Integer)
  case class Student(id:Int,nombre:String)
  case class Clasificacion(id_estudiante:Int, asignatura:String, calificacion:Integer)  
  val seqE=Buil_Seq_estudiantes()
  val spark =Create_Session_Spark("Estudiante Application")
  val sc: SparkContext=spark.sparkContext
  sc.setLogLevel("ERROR")
  
  def ejercicio1():Unit ={
     
      val df=Exercice_1(seqE)(spark)
      import spark.implicits._
      /* Excercice 1  a-) Print Schema */
      print("Excercice 1")
      print("a-) Print Schema\n")
      df.printSchema()

    /* Excercice 1  b-)  Filtra los estudiantes con una calificación mayor a 8. */ 
    print("b-) Filtra los estudiantes con una calificacion mayor a 8\n")
    val dfilterEdad=df.filter($"calificacion">8)
    dfilterEdad.show()

    /* Excercice 1  c-) Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente */
    print(" c-) Selecciona los nombres de los estudiantes y ordenalos por calificacion de forma descendente\n")
    val dforder=df.select("nombre","calificacion").orderBy($"calificacion".desc)
    dforder.show()
  }

  def ejercicio2():Unit={
    import spark.implicits._
    print("Excercice 2  a-) Define una funcion que determine si un numero es par o impar\n")
     val checkInParUDF = udf(Check_inPar _)
     val df = Exercice_1(seqE)(spark)
     df.select($"*",checkInParUDF($"calificacion").as("Par_or_Impar")).show()
  }
  
 
  def ejercicio3():Unit={
    import spark.implicits._
    print("Ejercicio 3: Joins y Agregaciones\n ")
    val joindf=Exercice_3_DF_(spark)
    val dfStatics=joindf.groupBy("id").agg(
      first($"nombre").as("Nombre"),
      avg($"calificacion").as("Promedio_Notas")
    )
    dfStatics.show()
  }


  def ejercicio5():Unit={
     import spark.implicits._
     print("Excercice 5 Procesamiento de archivos\n")
     val path="./ventas.csv"
     val dfventas=readCsv(path,spark)
     val dfresumen=dfventas.groupBy("id_producto").agg(
      sum($"cantidad" * $"precio_unitario").as("Total_Ventas")
    )
    dfresumen.show()
    
  }

  def ejercicio4():Unit={
    import spark.implicits._
    print("Ejercicio 4: De una Lista de palabras exponer las palabras y sus frequencias de ocurrencias\n ")
    val df=readCsv("./words.csv", spark)
    val wordsDF = df.select($"palabras")
    val wordsRDD = wordsDF.select("palabras").as[String].rdd
    val wordsCount: RDD[(String, Int)]=wordsRDD
     .map(word=>(word, 1))
     .reduceByKey(_ + _)

     wordsCount.collect().foreach{ case (word, conteo) => println(s"${word}: ${conteo}")}

  }


  def Sparkstop():Unit={
    spark.stop()
  }
  def Buil_Seq_estudiantes():Seq[Estudiante]={
       val seq_estudiantes=Seq(
      Estudiante("Anabel",12,"Femenino",7),
      Estudiante("Carlos", 15, "Masculino",6),
      Estudiante("Maria", 14, "Femenino",18),
      Estudiante("Rosalia", 25, "Femenino",20),
      Estudiante("Laura", 24, "Femenino",15),
      Estudiante("Alejandro", 35, "Masculino",19),
      Estudiante("Mario", 34, "Masculino",20)
      )
      seq_estudiantes
  }

  def Create_Session_Spark(name_app:String):SparkSession={
    val spark=SparkSession.builder().appName(name_app).master("local[*]").getOrCreate()
    spark
  }

  def Exercice_1(sequencia:Seq[Estudiante])(spark:SparkSession):DataFrame={
       import spark.implicits._
       sequencia.toDF()
  }

 def Check_inPar(number: Int):String={ if (number % 2 == 0) "Par" else "Impar"}
 
 def Exercice_3_DF_(spark:SparkSession):DataFrame={
  import spark.implicits._
  val df_student=Seq(
     Student(1, "Anabel"),
      Student(2, "Carlos"),
      Student(3, "María"),
      Student(4, "Rosalia"),
      Student(5, "Laura"),
      Student(6, "Alejandro"),
      Student(7, "Mario")
  ).toDF()
  val df_clasificacion = Seq(
      Clasificacion(1, "Matemáticas", 85),
      Clasificacion(1, "Ciencias", 90),
      Clasificacion(2, "Matemáticas", 78),
      Clasificacion(2, "Ciencias", 88),
      Clasificacion(3, "Matemáticas", 95),
      Clasificacion(3, "Ciencias", 80),
      Clasificacion(4, "Ciencias", 82),
      Clasificacion(4, "Matemáticas", 70),
      Clasificacion(5, "Matemáticas", 90),
      Clasificacion(5, "Ciencias", 75)
    ).toDF()
  
    val joindf = df_student.join(df_clasificacion, df_student("id") === df_clasificacion("id_estudiante"),"inner")
    joindf
 }

 def readCsv(path:String,spark:SparkSession):DataFrame={
    val dfcsvfile=spark.read
                     .option("header", true)
                     .option("inferSchema",true)
                     .csv(path)
      dfcsvfile
 }

 def WriteParquet():Unit={
  import spark.implicits._ 
  val df=Seq(
    ("Elegante sonoras palabras que representa tu mirada fija en mi","2024"),
    ("Como viento que sopla en la noche, a la sombra de las estrellas","2023"),
    ("otro verso","2024")
  ).toDF("texo","year")
  df.write.mode(SaveMode.Overwrite).partitionBy("year").parquet("src/files/cervatsParquet")

 }

 def ReadParquet():Unit={
    val d = spark.read.parquet("src/files/cervatsParquet")
                    .filter(
                      col("year") === "2024" &&
                      !col("texo").contains("otro")
                    
                    ) 
             
      
  println(d.show(truncate=false))
 }

 def UdfRegex(cadena:String):String={
     val datePattern: Regex = """\[(\d{2}/[A-Za-z]{3}/\d{4}):.*\]""".r 
     val result = datePattern.findFirstMatchIn(cadena).map(_.group(1))
     result.getOrElse("Fecha no Encontrada")
     
 } 

 def Readsparkcsv():Unit={
  import spark.implicits._
   val in = spark.read
                     .option("header", "false")
                      .option("inferSchema", "false")
                      .csv("src/files/apache.access.log")
                      .toDF("raw_log") 
  val dffinal =in  
   .withColumn("url", regexp_extract(col("raw_log"),"""^(\S+)""", 1))
   .withColumn("timestamp", regexp_extract(col("raw_log"), """\[(\d{2}/[A-Za-z]{3}/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4})\]""", 1))
   .withColumn("peticion", regexp_extract(col("raw_log"), """\"[A-Z]+\s+\S+\s+HTTP/\d\.\d\"\s+""", 0))
   .withColumn("statusCode", regexp_extract(col("raw_log"),  """\s+(\d{3})\s""" , 1))
   .withColumn("tiempoRespuesta", regexp_extract(col("raw_log"), """ (\d+)$""", 1))
   dffinal.show() 
      
}

  

}
