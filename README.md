![GitHub language count](https://img.shields.io/github/languages/count/Ramisoussi/Analysis_Soccer_Matches_using_SparkSQL?color=%23FFA500&logo=github)
![GitHub top language](https://img.shields.io/github/languages/top/Ramisoussi/Analysis_Soccer_Matches_using_SparkSQL?logo=Github)
![GitHub last commit](https://img.shields.io/github/last-commit/Ramisoussi/Analysis_Soccer_Matches_using_SparkSQL?logo=Github)

# Overview
Solving analytical questions on the [European Soccer Database](https://www.kaggle.com/datasets/hugomathien/soccer) <img src="https://img.icons8.com/doodle/15/null/football2--v1.png"/> using Spark and Scala. This features the use of SQL-like dataframe API to query structured data inside Spark programs. We aim to draw useful insights about different European leagues by leveraging different forms of Spark APIs.

# Table of Contents
* [Components](https://github.com/Ramisoussi/Analysis_Soccer_Matches_using_SparkSQL#Major-Components)
* [Analytical queries](https://github.com/Ramisoussi/Analysis_Soccer_Matches_using_SparkSQL#Analytical-Queries)
* [Installation steps](https://github.com/Ramisoussi/Analysis_Soccer_Matches_using_SparkSQL#Installation-steps)	
* [Mentions](https://github.com/Ramisoussi/Analysis_Soccer_Matches_using_SparkSQL#Mentions)


# Major Components
<p align="center">
	<a href="#">
		<img src="https://upload.wikimedia.org/wikipedia/commons/f/f3/Apache_Spark_logo.svg" alt="Apache Spark Logo" title="Apache Spark" width=180 hspace=10 />
	</a>
	<a href="#">
		<img src="https://upload.wikimedia.org/wikipedia/commons/3/39/Scala-full-color.svg" alt="Scala" title="Scala" width ="180" hspace=30/>
	</a>
</p>

# Analytical Queries

| Queries      | Solutions |
| ----------- | ----------- |
| [Easy level queries](/query_questions/easy_level_queries.md)      | [solutions](/src/main/scala/easy_level_queries.scala)     |
| [Medium level queries](/query_questions/medium_level_queries.md)  | [solutions](/src/main/scala/medium_level_queries.scala)   |
| [Hard level queries](/query_questions/hard_level_queries.md)      | [solutions](/src/main/scala/hard_level_queries.scala)     |


# Installation steps

1. Simply clone the repository
	```
	git clone https://github.com/Ramisoussi/Analysis_Soccer_Matches_using_SparkSQL.git
	```
2. In the repo, run `sbt` command to package the project as a JAR file
	```
	sbt package
	```
3. Once a user application is bundled, launch it using the spark submit script
	```
	spark-submit --class <main-class> --master <master-url> target/scala-2.12/match_analysis_2.12-1.0.jar
	```
`main-class` : The entry point for the application. It could be one of the following classes:
- `easy_level_query` 
- `medium_level_query`
- `hard_level_query`

`master-url` : The master URL for the cluster (e.g. `spark://23.195.26.187:7077`)

# Mentions
This project was featured with [Kaggle](https://www.kaggle.com/) open source datasets. Thank you for the listing.
	





