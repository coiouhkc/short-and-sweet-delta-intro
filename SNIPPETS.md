docker run --rm -it -p 4040:4040 -p 7077:7077 spark:4.0.1-java21-scala /opt/spark/bin/spark-shell --packages io.delta:delta-spark_2.13:4.0.0 --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.driver.port=7077

jbang export fatjar --force dataset_demo.java
zip -d dataset_demo-fatjar.jar META-INF/*.RSA META-INF
/*.DSA META-INF/*.SF


```shell
REPO_DIR=/tmp/repo
rm -fr $REPO_DIR
mkdir $REPO_DIR
cd $REPO_DIR
git init
git add .
git commit -m "Initial commit"
echo "hello" >> log.txt
git add .
git commit -m "1st commit"
echo "world" >> log.txt
git add .
git commit -m "2nd commit"
echo "!" >> log.txt
git add .
git commit -m "3rd commit"
```