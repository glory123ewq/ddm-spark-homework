package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }



  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    val enteredData = inputs.map(input => readData(input, spark))
      .map(inputTable => {
        val columns = inputTable.columns
        inputTable.flatMap(row => {
          for (i <- columns.indices) yield {
            (columns(i), row.getString(i))
          }
        })
      })
    val fullData = enteredData.reduce((firstSet, secondSet) => firstSet union secondSet);
    val groupedByValue = fullData.groupByKey(t => t._2)
      .mapGroups((_, iterator) => iterator.map(_._1).toSet);
    val finalINDs = groupedByValue.flatMap(Set => Set
        .map(currentAttr => (currentAttr, Set.filter(attr => !attr.equals(currentAttr)))))
      .groupByKey(row => row._1)
      .mapGroups((key, iterator) => (key, iterator.map(row => row._2).reduce((firstSet, secondSet) => firstSet.intersect(secondSet))))
      .collect();

    val sortedINDs = finalINDs.sortWith(_._1 < _._1)
    for((attribute, values) <- sortedINDs) {

      // Check if the set of values is not empty
      values match {
        case nonEmptyValues if nonEmptyValues.nonEmpty =>
          val valuesString = nonEmptyValues.mkString(",")
          println(s"$attribute -> $valuesString")
        case _ => // do nothing for empty values

      }
    }
  }


}
