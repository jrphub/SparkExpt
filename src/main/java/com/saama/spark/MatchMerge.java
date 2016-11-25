package com.saama.spark;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.rowset.RowSetWarning;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

import scala.Function1;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;

public class MatchMerge{
	
	//transient static final List<Column> colList = Arrays.asList(functions.col("id"),functions.col("name"),functions.col("age"),functions.col("cust_addr"),functions.col("city"),functions.col("company"));
	
	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		final List<String> colNames = Arrays.asList("id", "name", "age", "cust_addr", "city", "company");
		List<Column> colList = Arrays.asList(functions.col("id"),functions.col("name"),functions.col("age"),functions.col("cust_addr"),functions.col("city"),functions.col("company"));
		Seq colSeq = JavaConversions.asScalaBuffer(colList).toSeq();
		
		SparkConf conf = new SparkConf().setAppName("Match-Merge").setMaster("local[*]");
		JavaSparkContext jSparkCtxt = new JavaSparkContext(conf);
		SQLContext sqlCtxt = new SQLContext(jSparkCtxt);
		
		DataFrame data1 = sqlCtxt.read().json("data1.json");
		DataFrame data2 = sqlCtxt.read().json("data2.json");
		DataFrame data3 = sqlCtxt.read().json("data3.json");
		
		System.out.println("Initial Data : \n");
		data1.show();
		data2.show();
		data3.show();
		
		DataFrame allData1 = data1.withColumn("company", lit(null)).withColumn("cust_addr", lit(null));
		DataFrame allData2 = data2.withColumnRenamed("emp_id","id").withColumnRenamed("emp_name","name").withColumnRenamed("location","city").withColumn("age", lit(null)).withColumn("cust_addr", lit(null));
		DataFrame allData3 = data3.withColumnRenamed("cust_id","id").withColumnRenamed("cust_name","name").withColumnRenamed("cust_age","age").withColumn("city", lit(null)).withColumn("company", lit(null));
		
		System.out.println("After synching : \n");
		allData1.select(colSeq).show();
		allData2.select(colSeq).show();
		allData3.select(colSeq).show();
		
		DataFrame bin = allData1.select(colSeq).unionAll(allData2.select(colSeq)).unionAll(allData3.select(colSeq));
		System.out.println("After matching rule : \n");
		bin.orderBy("id").show();
		
		JavaPairRDD<String, Row> idRowPairRDD = bin.javaRDD().mapToPair(new PairFunction<Row, String, Row>() {
												
					public Tuple2<String, Row> call(Row row) throws Exception {
						return new Tuple2<String, Row>(row.getString(row.fieldIndex("id")), row);
					}});
		
		JavaPairRDD<String, Row> groupdIdRowPairRDD = idRowPairRDD.reduceByKey(new Function2<Row, Row, Row>() {
			
			 public Row call(Row row1, Row row2) throws Exception {

				DataFrame dataFrame = SQLContext.getOrCreate(null).createDataFrame(Arrays.asList(row1), row1.schema());
				
				for(String col : colNames){
					
					dataFrame = dataFrame.na().fill(row2.getString(row2.fieldIndex(col)), 
							new String[]{col});
				}
				
				return dataFrame.first();
			}
		  });
		
		JavaRDD<Row> finalRowsRDD = groupdIdRowPairRDD.map(new Function<Tuple2<String,Row>, Row>() {
			
			public Row call(Tuple2<String, Row> tuple) throws Exception {
				return tuple._2;
			}
		});
		
		System.out.println("After merging : \n");
		sqlCtxt.createDataFrame(finalRowsRDD, bin.schema()).orderBy("id").show();
		 
		//bin.filter("id = '101'").show();

		/*bin.registerTempTable("bin");
		sqlCtxt.sql("select id from (select id from bin group by id having count(*) > 1) binTemp").show();*/
		
		//bin.filter(functions.exp("id in (select id from bin group by id having count(id) > 1) binTemp")).show();;
		//sqlCtxtql("select id, name, age from bin where id in (select id from bin group by id having count(id) > 1)").show();
	}
}
