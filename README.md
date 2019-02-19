# spark-job-server-java-project
My first java project to use Spark Job Server(SJS)
https://github.com/spark-jobserver/spark-jobserver

When I tried to use SJS, I found most of the examples online were wrotten in Scala. 
There are only few of java example. All of them still development with old SJS API. 

This Java project loads exist Count Vectorizer Model and Logistic Regression Model to pre-process input comments and 
analysis it is positive or negative.

It takes three input parameters
1.comment to predict
2.cv model location
3.lr model location

Notes:
1. Start SJS by
   job-server-extras/reStart  (reStart is not working with sqlcontext)
2. Start JavaSparkContext instead of sparkContext
curl -d "" "localhost:8090/contexts/jcontext?context-factory=spark.jobserver.context.JavaSparkContextFactory"
3. Query example:
curl -d 'input.comments="yummy, great food, love love love",input.cvmodel="<cv location>",input.lrmodel="<lr location>"' "localhost:8090/jobs?appName=testml&classPath=spark.jobserver.SparkModelDeploy&context=jcontext&sync=true&context-factory=spark.jobserver.context.JavaSparkContextFactory&timeout=100000"
