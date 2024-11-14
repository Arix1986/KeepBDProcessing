package AppTest
import keepcod.App
import keepcod.App.Estudiante
import keepcod.App.Student
import keepcod.App.Clasificacion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File
import java.nio.file.{Files, Paths}
class AppTest extends AnyFunSuite with Matchers {
   
    
     test("La funcion debe retornar una secuencia de objetos Estudiante correctamente") {
             val build_seq =App.Buil_Seq_estudiantes() 
             build_seq should not be empty
             build_seq.foreach { estudiante =>
                    estudiante shouldBe a[Estudiante]
               }
     }

     test("Create_Session_Spark deberia crear una sesion valida de Spark") {
           val appName = "TestApp"
           val spark = App.Create_Session_Spark(appName)
           spark should not be null
           spark.conf.get("spark.app.name") shouldBe appName
           spark.stop()
     }
     test("Exercice_1 deberia convertir una secuencia de Estudiantes en un DataFrame"){
          val spark = SparkSession.builder().appName("Test Exercice_1").master("local[*]").getOrCreate()
          import spark.implicits._
          val estudiantes = Seq(
               Estudiante("Anabel",12,"Femenino",7),
               Estudiante("Ana", 22, "Femenino", 9),
               Estudiante("Luis", 19, "Masculino",8)
          )
          val df: DataFrame = App.Exercice_1(estudiantes)(spark)
          df.count() shouldBe estudiantes.size
          val Schema = Seq("nombre", "edad", "sexo", "calificacion")
          df.columns should contain theSameElementsAs Schema
     }
     test("Test de UDF que dado un number resulve si es Par o Impar "){

          val test= 3
          App.Check_inPar(test) shouldBe "Impar"
     }
      test("Exercice_3_DF_ debería realizar un join correctamente entre estudiantes y clasificaciones") {
        
          val spark = SparkSession.builder().appName("Test Exercice_3_DF_").master("local[*]").getOrCreate()
          import spark.implicits._
          val resultDF: DataFrame = App.Exercice_3_DF_(spark)

          val schema = Seq("id", "nombre", "id_estudiante", "asignatura", "calificacion")
          resultDF.columns should contain theSameElementsAs schema
          val datosEsperados = Seq(
               (1, "Anabel", 1, "Matemáticas", 85),
               (1, "Anabel", 1, "Ciencias", 90),
               (2, "Carlos", 2, "Matemáticas", 78),
               (2, "Carlos", 2, "Ciencias", 88),
               (3, "María", 3, "Matemáticas", 95),
               (3, "María", 3, "Ciencias", 80),
               (4, "Rosalia", 4, "Ciencias", 82),
               (4, "Rosalia", 4, "Matemáticas", 70),
               (5, "Laura", 5, "Matemáticas", 90),
               (5, "Laura", 5, "Ciencias", 75)
          ).toDF("id", "nombre", "id_estudiante", "materia", "calificacion")
          val resultData = resultDF.collect()
          val expectedData = datosEsperados.collect()
          resultData should contain theSameElementsAs expectedData
          spark.stop()
          }

       test("readCsv deberia leer correctamente un archivo CSV y devolver un DataFrame valido") {
  
              val spark = SparkSession.builder().appName("Test ReadCsv").master("local[*]").getOrCreate()
              import spark.implicits._
              val tempCsvPath = "test_data.csv"
              val csvData =
                    """id,nombre,edad
                    |1,Juan,20
                    |2,Ana,22
                    |3,Pedro,19
                    |""".stripMargin

              Files.write(Paths.get(tempCsvPath), csvData.getBytes)
              try {
                  
                    val df: DataFrame = App.readCsv(tempCsvPath, spark)
                    df.count() shouldBe 3
                    val schema = Seq("id", "nombre", "edad")
                    df.columns should contain theSameElementsAs schema
                    val datosEsperados = Seq(
                    (1, "Juan", 20),
                    (2, "Ana", 22),
                    (3, "Pedro", 19)
                    ).toDF("id", "nombre", "edad")

                    val datosResultantes = df.collect()
                    val datosEsperadosResultantes = datosEsperados.collect()

                    datosResultantes should contain theSameElementsAs datosEsperadosResultantes

               } finally {
                    new File(tempCsvPath).delete()
                    spark.stop()
               }
               }

}
