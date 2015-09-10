package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SimpleSpark {
	public static void main(String[] args) {
		String logFile = "../../Documents/index.html"; // Should be some file on
														// your system
		JavaSparkContext sc = new JavaSparkContext("local", "Simple App", "$YOUR_SPARK_HOME",
				new String[] { "target/simple-project-1.0.jar" });
		JavaRDD<String> logData = sc.textFile(logFile).cache();

		long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("Merkel");
			}
		}).count();

		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("Griechenland");
			}
		}).count();

		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	}
}
