import org.apache.spark.sql.functions.concat_ws
import scala.util.matching.Regex

// tweets hashtags 1
val raw_data = spark.read.text("C:/dataForProject/tweets_vote.json").as[String]
raw_data.createOrReplaceTempView("tweets")
val data_with_schema = spark.read.json(spark.sql("SELECT * FROM tweets").as[String])
data_with_schema.createOrReplaceTempView("tweets")
val hashtags_data = spark.sql("SELECT entities.hashtags.text FROM tweets")
hashtags_data.filter($"text".isNotNull).show()
hashtags_data.createOrReplaceTempView("tweets")

val concat_data = hashtags_data.withColumn("hashtags", concat_ws(" ", $"text"))
val filtered_concat_data = concat_data.filter(!($"hashtags" === ""))
filtered_concat_data.createOrReplaceTempView("tweets")

val data = spark.sql("SELECT hashtags FROM tweets")

val patternForRegex = new Regex("^\\s*[A-Za-z0-9]+(?:\\s+[A-Za-z0-9]+)*\\s*$")

val mapped_data = data.flatMap(sqlRow => (patternForRegex findAllIn sqlRow(0).toString).toList)
val reduced_data = mapped_data.groupByKey(_.toLowerCase)
val counts = reduced_data.count().orderBy($"count(1)".desc)
counts.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("C:/dataForProject/hashtagsCount")


// trump biden count 2
data_with_schema.createOrReplaceTempView("tweets")
val text_data = spark.sql("SELECT text FROM tweets")
val filtered_concat_data_text = text_data.filter(!($"text" === ""))
val trump_data = filtered_concat_data_text.filter($"text".rlike("trump"))
val biden_data = filtered_concat_data_text.filter($"text".rlike("biden"))
val kanye_data = filtered_concat_data_text.filter($"text".rlike("kanye"))

val countsTrump = trump_data.count()
val countsBiden = biden_data.count()
val countsKanye = kanye_data.count()
import java.io._
val pw = new PrintWriter(new File("C:/dataForProject/TrumpBidenCount/tweet_counts1.csv"))
pw.write("Trump,Biden,Kanye"+"\n" + countsTrump.toString + "," + countsBiden.toString+ "," + countsKanye.toString)
pw.close


// URL Count 3
data_with_schema.createOrReplaceTempView("tweets")
val urls = spark.sql("SELECT entities.urls.expanded_url FROM tweets").withColumn("expanded_url", concat_ws(" ", $"expanded_url"))
val filterUrls = urls.filter(!($"expanded_url" === ""))

val finalurl = filterUrls.select(substring(col("expanded_url"),0, 25).as("expanded_url"))
finalurl.show(10)
val patternForUrl = new Regex("(www|http|https)\\S+")
val mapped_data_urls = finalurl.flatMap(sqlRow => (patternForUrl findAllIn sqlRow(0).toString).toList)
val reduced_data_urls = mapped_data_urls.groupByKey(_.toLowerCase)
val countsURLs = reduced_data_urls.count().orderBy($"count(1)".desc)


countsURLs.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("C:/dataForProject/urlsCount3")


/* hash tag follower 4 */
data_with_schema.createOrReplaceTempView("tweets")
val hashtagfollower = spark.sql("SELECT entities.hashtags.text , user.followers_count as count FROM tweets order by  user.followers_count desc")
hashtagfollower.filter($"count".isNotNull)
val newhashtagfollower = hashtagfollower.withColumn("hashtags", concat_ws(" ", $"text"))
val newFilteredhashtagfollower = newhashtagfollower.filter(!($"hashtags" === ""))
newFilteredhashtagfollower.createOrReplaceTempView("tweets")

val hashtagfollowerFinal = spark.sql("SELECT hashtags, count FROM tweets")  

hashtagfollowerFinal.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("C:/dataForProject/hashtagfollowerCount")



/* url follower 5 */
data_with_schema.createOrReplaceTempView("tweets")
val urlfollower = spark.sql("SELECT entities.urls.expanded_url , user.followers_count as count FROM tweets order by user.followers_count desc")
urlfollower.filter($"count".isNotNull)
val newurlfollower = urlfollower.withColumn("expanded_url", concat_ws(" ", $"expanded_url"))
val newFilteredurlfollower = newurlfollower.filter(!($"expanded_url" === ""))
newFilteredurlfollower.createOrReplaceTempView("tweets")
val urlfollowerFinal = spark.sql("SELECT expanded_url, count  FROM tweets")
urlfollowerFinal.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("C:/dataForProject/urlfollowerCount")


/* language count 6 */
data_with_schema.createOrReplaceTempView("tweets")

val language = spark.sql("SELECT lang FROM tweets")
language.filter($"lang".isNotNull)
val newFilteredlanguage = language.filter(!($"lang" === ""))
newFilteredlanguage.createOrReplaceTempView("tweets")

val languageFinal = spark.sql("SELECT lang FROM tweets")
val pattern_language = new Regex("^\\s*[A-Za-z0-9]+(?:\\s+[A-Za-z0-9]+)*\\s*$")

val counts_language = languageFinal.flatMap(sqlRow => (pattern_language findAllIn sqlRow(0).toString).toList)
val countsreduced = counts_language.groupByKey(_.toLowerCase)
val languageFinalCounts = countsreduced.count().orderBy($"count(1)".desc)

languageFinalCounts.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("C:/dataForProject/languageCount")


/* language followers 7 */
data_with_schema.createOrReplaceTempView("tweets")

val languageFollowers = spark.sql("SELECT lang,  user.followers_count as followers FROM tweets")
languageFollowers.filter($"lang".isNotNull)
languageFollowers.filter($"followers".isNotNull)
val newFilteredlanguageFollowers = languageFollowers.filter(!($"lang" === ""))
newFilteredlanguageFollowers.createOrReplaceTempView("tweets")
val languageFollowersFinal = spark.sql("SELECT lang, followers FROM tweets")
val countFinal = languageFollowersFinal.groupBy("lang").sum()

countFinal.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("C:/dataForProject/languageFollowersCount")

/* timelineOfLang 8 */
data_with_schema.createOrReplaceTempView("tweets")

val timelineOfLang = spark.sql("SELECT created_at as time , user.followers_count as followers, lang FROM tweets")
timelineOfLang.filter($"lang".isNotNull)
val newFilteredtimelineOfLang = timelineOfLang.filter(!($"lang" === ""))
newFilteredtimelineOfLang.createOrReplaceTempView("tweets")
val timelineOfLangFinal = spark.sql("SELECT lang,time,followers FROM tweets")
import org.apache.spark.sql.functions._
val  timelineOfLangFinal2 = timelineOfLangFinal.select(col("tweets.lang"),col("tweets.followers"),col("tweets.time").as("time"))
timelineOfLangFinal2.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("C:/dataForProject/timelineOfLang")
     

/* timeline of followers 9 */
data_with_schema.createOrReplaceTempView("tweets")

val timelineOfLang2 = spark.sql("SELECT created_at as time , user.followers_count as followers FROM tweets")
timelineOfLang2.filter($"followers".isNotNull)
val newFilteredtimelineOfLang2 = timelineOfLang2.filter(!($"followers" === ""))
newFilteredtimelineOfLang2.createOrReplaceTempView("tweets")
val timelineOfLangFinal3 = spark.sql("SELECT time,followers FROM tweets")
import org.apache.spark.sql.functions._
val  timelineOfLangFinal4 = timelineOfLangFinal3.select(col("tweets.followers"),col("tweets.time").as("time"))
timelineOfLangFinal4.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("C:/dataForProject/timelineOfLangFollowers")
  
/* user location count 10 */


data_with_schema.createOrReplaceTempView("tweets")

val location = spark.sql("SELECT user.location as location FROM tweets")
location.filter($"location".isNotNull)
val newFilteredlocation = location.filter(!($"location" === ""))
newFilteredlocation.createOrReplaceTempView("tweets")

val locationFinal = spark.sql("SELECT location FROM tweets")
val pattern_location = new Regex("^\\s*[A-Za-z0-9]+(?:\\s+[A-Za-z0-9]+)*\\s*$")

val mapped_location = locationFinal.flatMap(sqlRow => (pattern_location findAllIn sqlRow(0).toString).toList)
val reduced_location = mapped_location.groupByKey(_.toLowerCase)
val counts_location = reduced_location.count().orderBy($"count(1)".desc)
counts_location.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("C:/dataForProject/locationCount")
