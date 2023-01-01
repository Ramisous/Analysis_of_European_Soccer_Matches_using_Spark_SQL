# Easy Level Queries

__Problem:__ Given csv files with football matches information, provide some insights using SparkSQL APIs 
    
__Driver/Main class:__ `easy_level_queries`

__Solution Package:__ *[src/main/scala/easy_level_queries.scala](https://github.com/Ramisous/Analysis_of_European_Soccer_Matches_using_Spark_SQL/blob/main/src/main/scala/easy_level_queries.scala)*

# Questions
You have the following tab delimited csv files [matchs.csv](https://github.com/Ramisous/Analysis_of_European_Soccer_Matches_using_Spark_SQL/blob/main/data/matchs.csv), [league.csv](https://github.com/Ramisous/Analysis_of_European_Soccer_Matches_using_Spark_SQL/blob/main/data/league.csv), [team.csv](https://github.com/Ramisous/Analysis_of_European_Soccer_Matches_using_Spark_SQL/blob/main/data/team.csv), [country.csv](https://github.com/Ramisous/Analysis_of_European_Soccer_Matches_using_Spark_SQL/blob/main/data/country.csv)

Using Spark and SparkSQL perform the following tasks: 
1. Select and show all the matches of FC Barcelona vs Real Madrid in chronological order of the seasons
    ``` 
    +---------+------------+-----+------------+
    |season   |Home team   |Score|Away team   |
    +---------+------------+-----+------------+
    |2008/2009|FC Barcelona|2 - 0|Real Madrid |
    |2008/2009|Real Madrid |2 - 6|FC Barcelona|
    |2009/2010|FC Barcelona|1 - 0|Real Madrid |
    |2009/2010|Real Madrid |0 - 2|FC Barcelona|
    |2010/2011|FC Barcelona|5 - 0|Real Madrid |
    |2010/2011|Real Madrid |1 - 1|FC Barcelona|
    |2011/2012|Real Madrid |1 - 3|FC Barcelona|
    |2011/2012|FC Barcelona|1 - 2|Real Madrid |
    |2012/2013|FC Barcelona|2 - 2|Real Madrid |
    |2012/2013|Real Madrid |2 - 1|FC Barcelona|
    |2013/2014|FC Barcelona|2 - 1|Real Madrid |
    |2013/2014|Real Madrid |3 - 4|FC Barcelona|
    |2014/2015|Real Madrid |3 - 1|FC Barcelona|
    |2014/2015|FC Barcelona|2 - 1|Real Madrid |
    |2015/2016|Real Madrid |0 - 4|FC Barcelona|
    |2015/2016|FC Barcelona|1 - 2|Real Madrid |
    +---------+------------+-----+------------+
    ```
2. Select and show the teams playing in the English league in the 2013/2014 season
    ``` 
    +--------------------+
    |Team                |
    +--------------------+
    |Swansea City        |
    |Sunderland          |
    |Manchester United   |
    |Cardiff City        |
    |Arsenal             |
    |Stoke City          |
    |Newcastle United    |
    |Crystal Palace      |
    |Aston Villa         |
    |Manchester City     |
    |Fulham              |
    |Southampton         |
    |Tottenham Hotspur   |
    |Liverpool           |
    |Norwich City        |
    |West Bromwich Albion|
    |Chelsea             |
    |Hull City           |
    |West Ham United     |
    |Everton             |
    +--------------------+
    ``` 
3. Select and show the top 3 teams that scored the most goals during the 2012/2013 season
    ``` 
    +----------------+----------------------+
    |            Team|Number_of_goals_scored|
    +----------------+----------------------+
    |    FC Barcelona|                   115|
    |     Real Madrid|                   103|
    |FC Bayern Munich|                    98|
    +----------------+----------------------+
    ``` 
4. Select and show the top 3 teams that conceded the lowest number of goals during the 2015/2016 season
    ``` 
    +-------------------+------------------------+
    |               Team|Number_of_goals_conceded|
    +-------------------+------------------------+
    |   FC Bayern Munich|                      17|
    |    Atlético Madrid|                      18|
    |Paris Saint-Germain|                      19|
    +-------------------+------------------------+
    ``` 


5. Select and show the results of the last stage games in the English league
    ``` 
    +---------+-----+--------------------+-----+-----------------------+
    |season   |stage|Home team           |Score|Away team              |
    +---------+-----+--------------------+-----+-----------------------+
    |2011/2012|38   |Chelsea             |2 - 1|Blackburn Rovers       |
    |2011/2012|38   |Everton             |3 - 1|Newcastle United       |
    |2011/2012|38   |Manchester City     |3 - 2|Queens Park Rangers    |
    |2011/2012|38   |Norwich City        |2 - 0|Aston Villa            |
    |2011/2012|38   |Stoke City          |2 - 2|Bolton Wanderers       |
    |2011/2012|38   |Sunderland          |0 - 1|Manchester United      |
    |2011/2012|38   |Swansea City        |1 - 0|Liverpool              |
    |2011/2012|38   |Tottenham Hotspur   |2 - 0|Fulham                 |
    |2011/2012|38   |West Bromwich Albion|2 - 3|Arsenal                |
    |2011/2012|38   |Wigan Athletic      |3 - 2|Wolverhampton Wanderers|
    +---------+-----+--------------------+-----+-----------------------+
    ``` 

6. Select and show the games of FC barcelona in the 2015/2016 season
    ``` 
    +---------+-----+-----------------------+-----+-------------------------+
    |season   |stage|Home_team              |Score|Away_team                |
    +---------+-----+-----------------------+-----+-------------------------+
    |2015/2016|1    |Athletic Club de Bilbao|0 - 1|FC Barcelona             |
    |2015/2016|2    |FC Barcelona           |1 - 0|Málaga CF                |
    |2015/2016|3    |Atlético Madrid        |1 - 2|FC Barcelona             |
    |2015/2016|4    |FC Barcelona           |4 - 1|Levante UD               |
    |2015/2016|5    |RC Celta de Vigo       |4 - 1|FC Barcelona             |
    |2015/2016|6    |FC Barcelona           |2 - 1|UD Las Palmas            |
    |2015/2016|7    |Sevilla FC             |2 - 1|FC Barcelona             |
    +---------+-----+-----------------------+-----+-------------------------+
    ``` 

7. Select and show the ranking of leagues according to the number of goals scored
    ``` 
    +----------------------+----------------------+
    |League_name           |Number_of_goals_scored|
    +----------------------+----------------------+
    |Italy Serie A         |1018                  |
    |Spain LIGA BBVA       |1009                  |
    |England Premier League|975                   |
    |France Ligue 1        |947                   |
    |Germany 1. Bundesliga |843                   |
    +----------------------+----------------------+
    ``` 
