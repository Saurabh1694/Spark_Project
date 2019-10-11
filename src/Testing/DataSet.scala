package Testing

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


object DataSet {

  def main(args: Array[String]) {

    //System.setProperty("hadoop.home.dir", "D:\\Software\\hadoop-2.5.0-cdh5.3.2")
   // System.setProperty("spark.sql.warehouse.dir", "file:/D:/Big data/Software/spark-2.4.0-bin-hadoop2.7/spark-2.4.0-bin-hadoop2.7/spark-warehouse")

    val spark = SparkSession
      .builder
      .appName("DataSet")
      .master("local")
      .getOrCreate()

//val df = spark.read.format("csv").option("header","true").load("D:/Big data/ProgramingData/ColumnTesting/DuplicateData.csv")

val person=spark.read.format("csv").option("header","true").load("D:/Big data/ProgramingData/ColumnTesting/dataframe/person.csv")

/* Data of person table : 
 * 
ID	NAME	GRADUATE_PROGRAM	SPARK_STATUS
0	SAURABH	0	100
1	KETAN	1	500
2	GARVIT	1	250

* */

val graduateProgram=spark.read.format("csv").option("header","true").load("D:/Big data/ProgramingData/ColumnTesting/dataframe/graduateProgram.csv")
/*
ID	DEGREE	DEPARTMENT	SCHOOL
0	MASTER	CS	S.V.J.INTER
2	MASTER	EC	K.V.M
1	PH.D	IT	G.I.C
*/

val sparkStatus=spark.read.format("csv").option("header","true").load("D:/Big data/ProgramingData/ColumnTesting/dataframe/sparkStatus.csv")

/*
ID	STATUS
500	VICE PRESIDENT
250	PMC MEMBER
100	CONTRIBUTOR
*/

// **************************INNER JOINS **************************************

val innerJoin=person.col("GRADUATE_PROGRAM") === graduateProgram.col("ID")
//person.join(graduateProgram,innerJoin).show()

/* ouput inner join
2019-10-10 23:39:45 INFO  DAGScheduler:54 - Job 4 finished: show at DataSet.scala:32, took 0.062486 s
+---+-------+----------------+------------+---+------+----------+-----------+
| ID|   NAME|GRADUATE_PROGRAM|SPARK_STATUS| ID|DEGREE|DEPARTMENT|     SCHOOL|
+---+-------+----------------+------------+---+------+----------+-----------+
|  0|SAURABH|               0|         100|  0|MASTER|        CS|S.V.J.INTER|
|  1|  KETAN|               1|         500|  1|  PH.D|        IT|      G.I.C|
|  2| GARVIT|               1|         250|  1|  PH.D|        IT|      G.I.C|
+---+-------+----------------+------------+---+------+----------+-----------+

*/

// **************************OUTER JOINS **************************************

val jointype="outer"

val outerjoin=person.col("GRADUATE_PROGRAM")===graduateProgram.col("ID")
//person.join(graduateProgram,outerjoin,jointype).show()
/*

+----+-------+----------------+------------+----+------+----------+-----------+
|  ID|   NAME|GRADUATE_PROGRAM|SPARK_STATUS|  ID|DEGREE|DEPARTMENT|     SCHOOL|
+----+-------+----------------+------------+----+------+----------+-----------+
|   0|SAURABH|               0|         100|   0|MASTER|        CS|S.V.J.INTER|
|null|   null|            null|        null|null|  null|      null|       null|
|   1|  KETAN|               1|         500|   1|  PH.D|        IT|      G.I.C|
|   2| GARVIT|               1|         250|   1|  PH.D|        IT|      G.I.C|
|null|   null|            null|        null|   2|MASTER|        EC|      K.V.M|
+----+-------+----------------+------------+----+------+----------+-----------+
*/

// **************************LEFT OUTER JOINS **************************************


val joinType="left_outer"
val leftOuterJoin=person.col("GRADUATE_PROGRAM")===graduateProgram.col("ID")
//graduateProgram.join(person,leftOuterJoin,joinType).show()
/*
+---+------+----------+-----------+----+-------+----------------+------------+
| ID|DEGREE|DEPARTMENT|     SCHOOL|  ID|   NAME|GRADUATE_PROGRAM|SPARK_STATUS|
+---+------+----------+-----------+----+-------+----------------+------------+
|  0|MASTER|        CS|S.V.J.INTER|   0|SAURABH|               0|         100|
|  2|MASTER|        EC|      K.V.M|null|   null|            null|        null|
|  1|  PH.D|        IT|      G.I.C|   2| GARVIT|               1|         250|
|  1|  PH.D|        IT|      G.I.C|   1|  KETAN|               1|         500|
+---+------+----------+-----------+----+-------+----------------+------------+
*/

// **************************RIGHT OUTER JOINS **************************************

val joinType1="right_outer"
val rightOuterJoin=person.col("GRADUATE_PROGRAM")===graduateProgram.col("ID")
//person.join(graduateProgram,rightOuterJoin,joinType1).show()

/*
+----+-------+----------------+------------+---+------+----------+-----------+
|  ID|   NAME|GRADUATE_PROGRAM|SPARK_STATUS| ID|DEGREE|DEPARTMENT|     SCHOOL|
+----+-------+----------------+------------+---+------+----------+-----------+
|   0|SAURABH|               0|         100|  0|MASTER|        CS|S.V.J.INTER|
|null|   null|            null|        null|  2|MASTER|        EC|      K.V.M|
|   2| GARVIT|               1|         250|  1|  PH.D|        IT|      G.I.C|
|   1|  KETAN|               1|         500|  1|  PH.D|        IT|      G.I.C|
+----+-------+----------------+------------+---+------+----------+-----------+

*/


// **************************LEFT SEMI JOINS **************************************

val joinType2="left_semi"
val leftSemiJoin=person.col("GRADUATE_PROGRAM")===graduateProgram.col("ID")
//graduateProgram.join(person,leftSemiJoin,joinType2).show()

/*
+---+------+----------+-----------+
| ID|DEGREE|DEPARTMENT|     SCHOOL|
+---+------+----------+-----------+
|  0|MASTER|        CS|S.V.J.INTER|
|  1|  PH.D|        IT|      G.I.C|
+---+------+----------+-----------+
*/


// **************************LEFT SEMI JOINS **************************************

val joinType3="left_anti"
val leftAntiJoin=person.col("GRADUATE_PROGRAM")===graduateProgram.col("ID")
//graduateProgram.join(person,leftAntiJoin,joinType3).show()

/*
+---+------+----------+------+
| ID|DEGREE|DEPARTMENT|SCHOOL|
+---+------+----------+------+
|  2|MASTER|        EC| K.V.M|
+---+------+----------+------+

*/


// **************************CROSS JOINS **************************************

val joinType4="cross"
val crossJoin=person.col("GRADUATE_PROGRAM")===graduateProgram.col("ID")
//person.join(graduateProgram,crossJoin,joinType4).show()
person.crossJoin(graduateProgram).show()

/*
+----+-------+----------------+------------+---+------+----------+-----------+
|  ID|   NAME|GRADUATE_PROGRAM|SPARK_STATUS| ID|DEGREE|DEPARTMENT|     SCHOOL|
+----+-------+----------------+------------+---+------+----------+-----------+
|   0|SAURABH|               0|         100|  0|MASTER|        CS|S.V.J.INTER|
|   0|SAURABH|               0|         100|  2|MASTER|        EC|      K.V.M|
|   0|SAURABH|               0|         100|  1|  PH.D|        IT|      G.I.C|
|   1|  KETAN|               1|         500|  0|MASTER|        CS|S.V.J.INTER|
|   1|  KETAN|               1|         500|  2|MASTER|        EC|      K.V.M|
|   1|  KETAN|               1|         500|  1|  PH.D|        IT|      G.I.C|
|   2| GARVIT|               1|         250|  0|MASTER|        CS|S.V.J.INTER|
|   2| GARVIT|               1|         250|  2|MASTER|        EC|      K.V.M|
|   2| GARVIT|               1|         250|  1|  PH.D|        IT|      G.I.C|
|null|   null|            null|        null|  0|MASTER|        CS|S.V.J.INTER|
|null|   null|            null|        null|  2|MASTER|        EC|      K.V.M|
|null|   null|            null|        null|  1|  PH.D|        IT|      G.I.C|
+----+-------+----------------+------------+---+------+----------+-----------+



*/

  }
}