package it.reply.data.devops

import it.reply.data.pasquali.Storage
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

case class KuduStorage(storage : Storage) {

  val UDFtoLong = udf { u: String => u.toLong }
  val UDFtoDouble = udf { u: String => u.toDouble }

  val spark : SparkSession = storage.spark
  import spark.implicits._

  def storeRatingsToKudu(filename : String) : Unit = {

    val table = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(filename)
      .withColumn("u", UDFtoLong('userid))
      .withColumn("m", UDFtoLong('movieid))
      .withColumn("r", UDFtoDouble('rating))
      .withColumn("time", 'timestamp)
      .drop("userid")
      .drop("movieid")
      .drop("rating")
      .drop("timestamp")
      .select('u, 'm, 'r, 'time)
      .toDF("userid", "movieid", "rating", "time")

    table.printSchema()

    storage.upsertKuduRows(table, "default.kudu_ratings")
  }

  def storeLinksToKudu(filename: String): Unit = {

    val table = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(filename)
      .withColumn("u", UDFtoLong('movieid))
      .drop("userid")
      .select('u, 'imdbid, 'tmdbid)
      .toDF("movieid", "imdbid", "tmdbid")

    table.printSchema()

    storage.upsertKuduRows(table, "default.kudu_links")

  }

  def storeTagsToKudu(filename: String) : Unit = {

    val table = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(filename)
      .withColumn("u", UDFtoLong('userid))
      .withColumn("m", UDFtoLong('movieid))
      .withColumn("time", 'timestamp)
      .drop("userid")
      .drop("movieid")
      .drop("timestamp")
      .select('u, 'm, 'tag, 'time)
      .toDF("userid", "movieid", "tag", "time")

    table.printSchema()

    storage.upsertKuduRows(table, "default.kudu_tags")

  }

  def storeMoviesToKudu(filename: String) : Unit = {

    val table = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(filename)
      .withColumn("m", UDFtoLong('movieid))
      .drop("movieid")
      .select('m, 'title, 'genres)
      .toDF("movieid", "title", "genres")

    table.printSchema()

    storage.upsertKuduRows(table, "default.kudu_movies")

  }
}
