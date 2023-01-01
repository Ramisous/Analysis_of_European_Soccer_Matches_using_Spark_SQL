# Medium Level Queries

__Problem:__ Given csv files with football matches information, provide some insights using SparkSQL APIs. 
    
__Driver/Main class:__ medium_level_queries

__Solution Package:__ *[src/main/scala/medium_level_queries.scala](https://github.com/Ramisous/Analysis_of_European_Soccer_Matches_using_Spark_SQL/blob/main/src/main/scala/medium_level_queries.scala)*

# Questions
You have the following tab delimited csv files [matchs.csv](https://github.com/Ramisous/Analysis_of_European_Soccer_Matches_using_Spark_SQL/blob/main/data/matchs.csv), [league.csv](https://github.com/Ramisous/Analysis_of_European_Soccer_Matches_using_Spark_SQL/blob/main/data/league.csv), [team.csv](https://github.com/Ramisous/Analysis_of_European_Soccer_Matches_using_Spark_SQL/blob/main/data/team.csv), [country.csv](https://github.com/Ramisous/Analysis_of_European_Soccer_Matches_using_Spark_SQL/blob/main/data/country.csv)

Using Spark and SparkSQL perform the following tasks: 
1. Select and show the win, draw and loss percentages when playing as a home team
    ``` 
    +----------------+---------------+----------+
    |Home_team_result|Number_of_games|percentage|
    +----------------+---------------+----------+
    |             Win|           6748|     46.27|
    |            Draw|           3739|     25.64|
    |            Loss|           4098|      28.1|
    +----------------+---------------+----------+
    ```
2. Select and show the teams relegated from the French league in the 2010-2011 season
    ``` 
    +----------------+
    |Relegated_team  |
    +----------------+
    |AC Arles-Avignon|
    |AS Monaco       |
    |RC Lens         |
    +----------------+
    ``` 
3. Select and show the relegated teams from the Spanish league in chronological order of the seasons
    ``` 
    +---------+-------------------------+
    |season   |Relegated_team           |
    +---------+-------------------------+
    |2008/2009|Real Betis Balompié      |
    |2008/2009|RC Recreativo            |
    |2008/2009|CD Numancia              |
    |2009/2010|CD Tenerife              |
    |2009/2010|Real Valladolid          |
    |2009/2010|Xerez Club Deportivo     |
    |2010/2011|UD Almería               |
    |2010/2011|Hércules Club de Fútbol  |
    |2010/2011|RC Deportivo de La Coruña|
    |2011/2012|Villarreal CF            |
    |2011/2012|Real Sporting de Gijón   |
    |2011/2012|Racing Santander         |
    |2012/2013|RCD Mallorca             |
    |2012/2013|RC Deportivo de La Coruña|
    |2012/2013|Real Zaragoza            |
    |2013/2014|Real Betis Balompié      |
    |2013/2014|Real Valladolid          |
    |2013/2014|CA Osasuna               |
    |2014/2015|Córdoba CF               |
    |2014/2015|UD Almería               |
    +---------+-------------------------+
    ``` 
4. Select and show the teams that have not been relegated from the French league over the seasons
    ``` 
    +----------------------+-----------------+
    |Team                  |Number_of_seasons|
    +----------------------+-----------------+
    |Olympique Lyonnais    |8                |
    |FC Lorient            |8                |
    |Paris Saint-Germain   |8                |
    |AS Saint-Étienne      |8                |
    |Stade Rennais FC      |8                |
    |OGC Nice              |8                |
    |LOSC Lille            |8                |
    |Olympique de Marseille|8                |
    |Girondins de Bordeaux |8                |
    |Toulouse FC           |8                |
    +----------------------+-----------------+
    ``` 


5. Select and show the average length of the leagues' seasons in days
    ``` 
    +----------------------+--------------+
    |League_name           |Average_length|
    +----------------------+--------------+
    |France Ligue 1        |287 days      |
    |England Premier League|276 days      |
    |Spain LIGA BBVA       |270 days      |
    |Italy Serie A         |266 days      |
    |Germany 1. Bundesliga |273 days      |
    +----------------------+--------------+
    ``` 

6. Select and show the month having the most games played in each league
    ``` 
    +----------------------+------------------+
    |League_name           |Most_crowded_month|
    +----------------------+------------------+
    |England Premier League|12                |
    |France Ligue 1        |4                 |
    |Germany 1. Bundesliga |4                 |
    |Italy Serie A         |4                 |
    |Spain LIGA BBVA       |4                 |
    +----------------------+------------------+
    ``` 
