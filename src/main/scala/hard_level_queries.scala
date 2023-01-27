import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object hard_level_queries {
    
    def main(args: Array[String]): Unit = {

        val spark:SparkSession = SparkSession.builder()
        .appName("Match_analysis")
        .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        
        val country = spark.read.option("header",true).option("inferSchema",true).csv("data/country.csv")
        val league = spark.read.option("header",true).option("inferSchema",true).csv("data/league.csv")
        val matchs = spark.read.option("header",true).option("inferSchema",true).csv("data/matchs.csv")
        val team = spark.read.option("header",true).option("inferSchema",true).csv("data/team.csv") 

        //1. Find and show the final table of the Spanish league for the 2010/2011 season

        println("SHOWING: The final table of the Spanish league for the 2010/2011 season")

        matchs.join(country ,col("country_id") === country("id"))
        .filter(col("name") === "Spain" && col("season") === "2010/2011")
        .withColumn("points",
        when(col("home_team_goal") > col("away_team_goal"),3)
        .when(col("home_team_goal") === col("away_team_goal"),1)
        .otherwise(0))
        .select(col("home_team_id").alias("team_id"), col("points"))
        .union(
            matchs.join(country ,col("country_id") === country("id"))
        .filter(col("name") === "Spain" && col("season") === "2010/2011")
        .withColumn("points",
        when(col("home_team_goal") < col("away_team_goal"),3)
        .when(col("home_team_goal") === col("away_team_goal"),1)
        .otherwise(0))
        .select(col("away_team_id").alias("team_id"), col("points"))
        )
        .groupBy("team_id").agg(sum("points").alias("Points"))
        .join(team, Seq("team_id"))
        .orderBy(desc("Points"))
        .withColumn("Position",row_number.over(Window.partitionBy().orderBy(desc("Points"))))
        .select(col("Position"), col("team_long_name").alias("Team") , col("Points"))
        .show(false)  

        //2. Find and show the final table with goal differences of the English league for the 2011/2012 season

        println("SHOWING: The final table with goal differences of the English league for the 2011/2012 season")

        matchs.join(country ,col("country_id") === country("id"))
        .filter(col("name") === "England" && col("season") === "2011/2012")
        .withColumn("points",
        when(col("home_team_goal") > col("away_team_goal"),3)
        .when(col("home_team_goal") === col("away_team_goal"),1)
        .otherwise(0))
        .select(col("home_team_id").alias("team_id"), col("points"), col("home_team_goal").alias("goals_scored"),col("away_team_goal").alias("goals_conceded"))
        .union(
            matchs.join(country ,col("country_id") === country("id"))
        .filter(col("name") === "England" && col("season") === "2011/2012")
        .withColumn("points",
        when(col("home_team_goal") < col("away_team_goal"),3)
        .when(col("home_team_goal") === col("away_team_goal"),1)
        .otherwise(0))
        .select(col("away_team_id").alias("team_id"), col("points"),  col("away_team_goal").alias("goals_scored"),col("home_team_goal").alias("goals_conceded"))
        )
        .groupBy("team_id").agg(
            sum("points").alias("Points"),
            sum("goals_scored").as("Goals_for"),
            sum("goals_conceded").as("Goals_against")
            )
        .withColumn("Goal_difference", col("Goals_for") - col("Goals_against"))    
        .join(team, Seq("team_id"))
        .withColumn("Position",row_number.over(Window.partitionBy().orderBy(desc("Points"), desc("Goal_difference"))))
        .select(col("Position"), col("team_long_name").alias("Team") , col("Points"), col("Goal_difference"),col("Goals_for"), col("Goals_against"))
        .show(false)

        //3. Select and show the winner of the English league of each season

        println("SHOWING: The winner of the English league of each season")

        matchs.join(country ,col("country_id") === country("id"))
        .filter(col("name") === "England")
        .withColumn("points",
        when(col("home_team_goal") > col("away_team_goal"),3)
        .when(col("home_team_goal") === col("away_team_goal"),1)
        .otherwise(0))
        .select(col("home_team_id").alias("team_id"), col("points"), col("home_team_goal").alias("goals_scored"),col("away_team_goal").alias("goals_conceded"),col("season"))
        .union(
            matchs.join(country ,col("country_id") === country("id"))
        .filter(col("name") === "England")
        .withColumn("points",
        when(col("home_team_goal") < col("away_team_goal"),3)
        .when(col("home_team_goal") === col("away_team_goal"),1)
        .otherwise(0))
        .select(col("away_team_id").alias("team_id"), col("points"),  col("away_team_goal").alias("goals_scored"),col("home_team_goal").alias("goals_conceded"),col("season"))
        )
        .groupBy("team_id","season").agg(
            sum("points").alias("total_points"),
            sum("goals_scored").as("goals_for"),
            sum("goals_conceded").as("goals_against")
            )
        .withColumn("goals_difference", col("goals_for") - col("goals_against"))  
        .withColumn("max",first("team_id").over(Window.partitionBy("season").orderBy(desc("total_points"),desc("goals_difference"))))
        .where(col("team_id") === col("max"))
        .join(team, Seq("team_id"))
        .select(col("team_long_name").alias("Winner") , col("season"))
        .show(false)


        //4. Find and show the team with the longest winning streak

        println("SHOWING: The team with the longest winning streak") 

        matchs.withColumn("win_as_null",
        when(col("home_team_goal") <= col("away_team_goal"),1))
        .select(col("home_team_id").alias("team_id"), col("win_as_null"), col("date"))
        .union(
            matchs.withColumn("win_as_null",
        when(col("home_team_goal") >= col("away_team_goal"),1))
        .select(col("away_team_id").alias("team_id"), col("win_as_null"), col("date"))
        )    
        .withColumn("rank",sum("win_as_null").over(Window.partitionBy("team_id").orderBy(col("date")).rowsBetween(Window.currentRow, Window.unboundedFollowing)))
        .filter(col("win_as_null").isNull)
        .groupBy("team_id","rank").agg(count("*").alias("Games_won_in_succession"))
        .withColumn("max",max("Games_won_in_succession").over(Window.partitionBy()))
        .where(col("Games_won_in_succession") === col("max"))
        .join(team,Seq("team_id"))
        .orderBy(desc("Games_won_in_succession"))
        .select(col("team_long_name").alias("Team"),col("Games_won_in_succession"))
        .show()


        //5. Find and show the team with the longest losing streak 

        println("SHOWING: The team with the longest losing streak") 

        matchs.withColumn("loss_as_null",
        when(col("home_team_goal") >= col("away_team_goal"),1))
        .select(col("home_team_id").alias("team_id"), col("loss_as_null"), col("date"))
        .union(
            matchs.withColumn("loss_as_null",
        when(col("home_team_goal") <= col("away_team_goal"),1))
        .select(col("away_team_id").alias("team_id"), col("loss_as_null"), col("date"))
        )    
        .withColumn("rank",sum("loss_as_null").over(Window.partitionBy("team_id").orderBy(col("date")).rowsBetween(Window.currentRow, Window.unboundedFollowing)))
        .filter(col("loss_as_null").isNull)
        .groupBy("team_id","rank").agg(count("*").alias("Games_lost_in_succession"))
        .withColumn("max",max("Games_lost_in_succession").over(Window.partitionBy()))
        .where(col("Games_lost_in_succession") === col("max"))
        .join(team,Seq("team_id"))
        .orderBy(desc("Games_lost_in_succession"))
        .select(col("team_long_name").alias("Team"),col("Games_lost_in_succession"))
        .show()
    
  }
}
