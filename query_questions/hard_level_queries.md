# Hard Level Queries

__Problem:__ Given csv files with football matches information, provide some insights using SparkSQL APIs 
    
__Driver/Main class:__ `hard_level_queries`

__Solution Package:__ *[src/main/scala/hard_level_queries.scala](https://github.com/Ramisous/Analysis_of_European_Soccer_Matches_using_Spark_SQL/blob/main/src/main/scala/hard_level_queries.scala)*

# Questions
You have the following tab delimited csv files [matchs.csv](https://github.com/Ramisous/Analysis_of_European_Soccer_Matches_using_Spark_SQL/blob/main/data/matchs.csv), [league.csv](https://github.com/Ramisous/Analysis_of_European_Soccer_Matches_using_Spark_SQL/blob/main/data/league.csv), [team.csv](https://github.com/Ramisous/Analysis_of_European_Soccer_Matches_using_Spark_SQL/blob/main/data/team.csv), [country.csv](https://github.com/Ramisous/Analysis_of_European_Soccer_Matches_using_Spark_SQL/blob/main/data/country.csv)

Using Spark and SparkSQL perform the following tasks: 
1. Find and show the final table of the Spanish league for the 2010/2011 season
    ``` 
    +--------+-------------------------+------+
    |Position|Team                     |Points|
    +--------+-------------------------+------+
    |1       |FC Barcelona             |96    |
    |2       |Real Madrid              |92    |
    |3       |Valencia CF              |71    |
    |4       |Villarreal CF            |62    |
    |5       |Atlético Madrid          |58    |
    |6       |Sevilla FC               |58    |
    |7       |Athletic Club de Bilbao  |58    |
    |8       |RCD Espanyol             |49    |
    |9       |CA Osasuna               |47    |
    |10      |Real Sporting de Gijón   |47    |
    |11      |Málaga CF                |46    |
    |12      |Racing Santander         |46    |
    |13      |Levante UD               |45    |
    |14      |Real Sociedad            |45    |
    |15      |Real Zaragoza            |45    |
    |16      |RCD Mallorca             |44    |
    |17      |Getafe CF                |44    |
    |18      |RC Deportivo de La Coruña|43    |
    |19      |Hércules Club de Fútbol  |35    |
    |20      |UD Almería               |30    |
    +--------+-------------------------+------+
    ```
2. Find and show the final table with goal differences of the English league for the 2011/2012 season
    ``` 
    +--------+-----------------------+------+---------------+---------+-------------+
    |Position|Team                   |Points|Goal_difference|Goals_for|Goals_against|
    +--------+-----------------------+------+---------------+---------+-------------+
    |1       |Manchester City        |89    |64             |93       |29           |
    |2       |Manchester United      |89    |56             |89       |33           |
    |3       |Arsenal                |70    |25             |74       |49           |
    |4       |Tottenham Hotspur      |69    |25             |66       |41           |
    |5       |Newcastle United       |65    |5              |56       |51           |
    |6       |Chelsea                |64    |19             |65       |46           |
    |7       |Everton                |56    |10             |50       |40           |
    |8       |Liverpool              |52    |7              |47       |40           |
    |9       |Fulham                 |52    |-3             |48       |51           |
    |10      |Swansea City           |47    |-7             |44       |51           |
    |11      |West Bromwich Albion   |47    |-7             |45       |52           |
    |12      |Norwich City           |47    |-14            |52       |66           |
    |13      |Sunderland             |45    |-1             |45       |46           |
    |14      |Stoke City             |45    |-17            |36       |53           |
    |15      |Wigan Athletic         |43    |-20            |42       |62           |
    |16      |Aston Villa            |38    |-16            |37       |53           |
    |17      |Queens Park Rangers    |37    |-23            |43       |66           |
    |18      |Bolton Wanderers       |36    |-31            |46       |77           |
    |19      |Blackburn Rovers       |31    |-30            |48       |78           |
    |20      |Wolverhampton Wanderers|25    |-42            |40       |82           |
    +--------+-----------------------+------+---------------+---------+-------------+
    ``` 
3. Select and show the winner of the English league of each season
    ``` 
    +-----------------+---------+
    |Winner           |season   |
    +-----------------+---------+
    |Manchester United|2008/2009|
    |Chelsea          |2009/2010|
    |Manchester United|2010/2011|
    |Manchester City  |2011/2012|
    |Manchester United|2012/2013|
    |Manchester City  |2013/2014|
    |Chelsea          |2014/2015|
    |Leicester City   |2015/2016|
    +-----------------+---------+
    ``` 
4. Find and show the team with the longest winning streak
    ``` 
    +----------------+-----------------------+
    |            Team|Games_won_in_succession|
    +----------------+-----------------------+
    |FC Bayern Munich|                     19|
    +----------------+-----------------------+
    ``` 


5. Find and show the team with the longest losing streak
    ``` 
   +----------------+------------------------+
   |            Team|Games_lost_in_succession|
   +----------------+------------------------+
   |Grenoble Foot 38|                      12|
   +----------------+------------------------+
    ``` 
