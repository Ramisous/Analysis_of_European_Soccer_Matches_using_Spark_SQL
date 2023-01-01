# Overview
Solving analytical questions on the [European Soccer Database](https://www.kaggle.com/datasets/hugomathien/soccer) using Spark and Scala. This features the use of SQL-like dataframe API to query structured data inside Spark programs. We aim to draw useful insights about different European leagues <img src="https://img.icons8.com/doodle/15/null/football2--v1.png"/>  by leveraging different forms of Spark APIs.

# Table of Contents


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


# Installation steps

1. Simply clone the repository
	```
	git clone https://github.com/Ramisous/Analysis_of_European_Soccer_Matches_using_Spark_SQL.git
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
	





