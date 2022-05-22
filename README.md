# Как запустить приложение
```bash
sbt compile && sbt package # чтобы записать в jar
spark-submit --class solution target/scala-2.13/solution_2.13-0.1.0.jar 
```
