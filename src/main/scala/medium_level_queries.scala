import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.expressions.Window


object medium_level_queries {

  def main(args: Array[String]): Unit = {


    val spark:SparkSession = SparkSession.builder()
    .appName("Match_analysis")
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    
    val country = spark.read.option("header",true).option("inferSchema",true).csv("data/country.csv")
    val league = spark.read.option("header",true).option("inferSchema",true).csv("data/league.csv")
    val matchs = spark.read.option("header",true).option("inferSchema",true).csv("data/matchs.csv")
    val team = spark.read.option("header",true).option("inferSchema",true).csv("data/team.csv")

    //1. Select and show the win, draw and loss percentages when playing as a home team

    println("SHOWING: The win, draw and loss percentages when playing as a home team")

    matchs.withColumn("Home_team_result", 
    when(col("home_team_goal") > col("away_team_goal"),"Win")
    .when(col("away_team_goal") > col("home_team_goal"),"Loss")
    .otherwise("Draw"))
    .groupBy("Home_team_result").agg(count("Home_team_result").alias("Number_of_games"))
    .withColumn("Percentage",  concat(round(lit(100) * col("Number_of_games") / sum("Number_of_games").over() ,2) , lit(" %")))
    .show()

    //2. Select and show the relegated teams from the French league in the 2010-2011 season
    
    println("SHOWING: The relegated teams from the French league in the 2010-2011 season")

    matchs.join(country ,col("country_id") === country("id"))
    .join(team, col("team_id") ===  col("home_team_id"))
    .filter(country("name") === "France" && col("season") === "2010/2011")
    .select(team("team_long_name").alias("Relegated_team")).distinct()
    .except(
        matchs.join(country ,col("country_id") === country("id"))
    .join(team, col("team_id") ===  col("home_team_id"))
    .filter(col("name") === "France" && col("season") === "2011/2012")
    .select(col("team_long_name").alias("Relegated_team")).distinct()
    ).show(false)
    
    //3. Select and show the relegated teams from the Spanish league in chronological order of the seasons

    println("SHOWING: The relegated teams from the Spanish league in chronological order of the seasons")

    matchs.join(country ,col("country_id") === country("id"))
    .filter(country("name") === "Spain")
    .select("home_team_id","season").distinct()
    .withColumn("current_year",substring(col("season"), 1,4).cast(IntegerType))
    .withColumn("next_year",lead("current_year",1).over(Window.partitionBy("home_team_id").orderBy("current_year")))
    .where((col("current_year") + 1 < col("next_year") || col("next_year").isNull) && col("season") =!= "2015/2016")
    .join(team,col("home_team_id") === team("team_id"))
    .orderBy("current_year")
    .select(col("season"), col("team_long_name").alias("Relegated_team"))
    .show(20,false)

    //4. Select and show the teams that have not been relegated from the French league over the seasons

    println("SHOWING: Teams that have not been relegated from the French league over the seasons")

    matchs.join(country ,col("country_id") === country("id"))
    .join(team, team("team_id") ===  col("home_team_id"))
    .filter(country("name") === "France")
    .select(col("team_long_name").alias("Team") , col("season")).distinct()
    .groupBy("Team").agg(count("season").alias("Number_of_seasons"))
    .where(col("Number_of_seasons") === matchs.select("season").distinct.count())
    .show(false)

    //5. Select and show the average length of the leagues' seasons in days
    println("SHOWING: Average length of the leagues seasons in days")

    matchs.join(league ,col("league_id") === league("id"))
    .groupBy(league("name"),col("season"))
    .agg(datediff(max(date_trunc("Day",col("date"))), min(date_trunc("Day",col("date")))).alias("league_length"))
    .groupBy("name").agg(avg(col("league_length")).cast(IntegerType).alias("Average_length"))
    .select(col("name").alias("League_name"), concat(col("Average_length"), lit(" days")).alias("Average_length"))
    .show(false)

    //6. Select and show the month having the most games played in each league

    println("SHOWING: The month having the most games played in each league")

    matchs.withColumn("Month",month(col("date")))
    .groupBy(col("league_id"),col("Month")).count()
    .withColumn("rank",rank().over(Window.partitionBy("league_id").orderBy(col("count").desc)))
    .where(col("rank") === 1)
    .join(league ,col("league_id") === league("id"))
    .select(col("name").alias("League_name") , col("Month").alias("Most_crowded_month"))
    .show(false)
      
  }
}
