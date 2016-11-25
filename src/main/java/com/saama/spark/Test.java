package com.saama.spark;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Test {

	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		Column col = new Column("salary");
		System.out.println(col.toString());

		SparkConf conf = new SparkConf().setAppName("Match-Merge").setMaster("local[*]");
		JavaSparkContext jSparkCtxt = new JavaSparkContext(conf);
		final SQLContext sqlCtxt = new SQLContext(jSparkCtxt);

		JavaRDD<Row> rows = jSparkCtxt.parallelize(Arrays.asList(RowFactory.create(100, "small"), 
									RowFactory.create(200, "large")));
		
		StructType salaryStruct = DataTypes.createStructType(
				new StructField[] { DataTypes.createStructField("salary", DataTypes.IntegerType, true),
						DataTypes.createStructField("type", DataTypes.StringType, true) });

		DataFrame df = sqlCtxt.createDataFrame(rows, salaryStruct);
		df.show();
	}
}
