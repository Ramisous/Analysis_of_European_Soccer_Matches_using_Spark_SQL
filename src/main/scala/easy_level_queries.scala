import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object easy_level_queries {
   
   def main(args: Array[String]): Unit = {
      
      val spark:SparkSession = SparkSession.builder()
      .appName("Match_analysis")
      .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")
      
      val country = spark.read.option("header",true).option("inferSchema",true).csv("data/country.csv")
      val league = spark.read.option("header",true).option("inferSchema",true).csv("data/league.csv")
      val matchs = spark.read.option("header",true).option("inferSchema",true).csv("data/matchs.csv")
      val team = spark.read.option("header",true).option("inferSchema",true).csv("data/team.csv")

      //1. Select and show all the matches of FC Barcelona vs Real Madrid in chronological order of the seasons 

      println("SHOWING: all the matches of 'FC Barcelona' vs 'Real Madrid' in chronological order of the seasons")

      matchs.join(team.as("team1"), col("home_team_id") ===  col("team_id"))
      .join(team.as("team2"), col("away_team_id") ===  col("team2.team_id"))
      .where((col("team1.team_long_name") isin ("FC Barcelona","Real Madrid")) && (col("team2.team_long_name") isin ("FC Barcelona","Real Madrid")))
      .orderBy("date")
      .select(col("season") , col("team1.team_long_name").alias("Home team"),
      concat(col("home_team_goal"), lit(" - "), col("away_team_goal")).alias("Score"),
      col("team2.team_long_name").alias("Away team"))
      .show(false)

      //2. Select and show the teams playing in the English league in the 2013/2014 season

      println("SHOWING: all the teams playing in the English league in 2013/2014 season")

      matchs.join(country ,col("country_id") === country("id"))
      .join(team, col("team_id") ===  col("home_team_id"))
      .filter(col("name") === "England" && col("season") === "2013/2014")
      .select(col("team_long_name").alias("Team")).distinct()
      .show(false)
   
      //3. Select and show the top 3 teams that scored the most goals during the 2012/2013 season

      println("SHOWING: Top 3 teams that scored the most goals during 2012/2013 season")

      matchs.filter(col("season") === "2012/2013")
      .select(col("home_team_id").alias("team_id"), col("home_team_goal").alias("goals"))
      .union(
         matchs.filter(col("season") === "2012/2013")
         .select(col("away_team_id").alias("team_id"), col("away_team_goal").alias("goals")) 
      )
      .groupBy("team_id").agg(sum("goals").alias("Number_of_goals_scored"))
      .join(team, Seq("team_id"))
      .select(col("team_long_name").alias("Team"), col("Number_of_goals_scored"))
      .orderBy(col("Number_of_goals_scored").desc)
      .show(3)

      //4. Select and show the top 3 teams that conceded the lowest number of goals during the 2015/2016 season

      println("SHOWING: Top 3 teams that conceded the lowest number of goals during 2015/2016 season")

      matchs.filter(col("season") === "2015/2016")
      .select(col("home_team_id").alias("team_id"), col("away_team_goal").alias("goals_conceded"))
      .union(
         matchs.filter(col("season") === "2015/2016")
         .select(col("away_team_id").alias("team_id"), col("home_team_goal").alias("goals_conceded")) 
      )
      .groupBy("team_id").agg(sum("goals_conceded").alias("Number_of_goals_conceded"))
      .join(team, Seq("team_id"))
      .select(col("team_long_name").alias("Team"), col("Number_of_goals_conceded"))
      .orderBy(col("Number_of_goals_conceded").asc)
      .show(3)

      //5. Select and show the results of the last stage games in the English league
      
      println("SHOWING: Results of the last stage games in the English league")

      matchs.join(team.as("team1"), col("home_team_id") ===  col("team_id"))
      .join(team.as("team2"), col("away_team_id") ===  col("team2.team_id"))
      .join(country ,col("country_id") === country("id"))
      .filter(country("name") === "England" && col("season") === "2011/2012")
      .withColumn("last_stage",max("stage").over(Window.partitionBy()))
      .where(col("stage") === col("last_stage"))
      .select(col("season"),col("stage"),col("team1.team_long_name").alias("Home team"),
      concat(col("home_team_goal"),lit(" - "),col("away_team_goal")).alias("Score"),col("team2.team_long_name").alias("Away team"))
      .show(false)

      //6. select and show the games of FC barcelona in the 2015/2016 season

      println("SHOWING: Games of FC barcelona in 2015/2016 season")

      matchs.join(team.as("team1"), col("home_team_id") ===  col("team_id"))
      .join(team.as("team2"), col("away_team_id") ===  col("team2.team_id"))
      .join(country ,col("country_id") === country("id"))
      .filter(col("team1.team_long_name") === "FC Barcelona" || col("team2.team_long_name") === "FC Barcelona")
      .filter(col("season") === "2015/2016")
      .orderBy(col("stage"))
      .select(col("season"),col("stage"),col("team1.team_long_name").alias("Home_team"),
      concat(col("home_team_goal"),lit(" - "),col("away_team_goal")).alias("Score"),
      col("team2.team_long_name").alias("Away_team"))
      .show(false)
      
      //7. Selects and show the ranking of leagues according to the number of goals scored in 2014/2015 season

      println("SHOWING: Ranking of leagues according to the number of goals scored in 2014/2015 season")

      matchs.filter(col("season") === "2014/2015")
      .groupBy("league_id").agg((sum("home_team_goal") + sum("away_team_goal")).alias("Number_of_goals_scored"))
      .join(league, col("league_id") === col("id"))
      .orderBy(col("Number_of_goals_scored").desc)
      .select(col("name").alias("League_name"),col("Number_of_goals_scored"))
      .show(false)

  }
}
