package org.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class TwitterApp {

	private static final String SEARCH_TERM = "nice";
	private static final String HOME_PATH = "/home/berthold/";
	private static PolarityBasic polarityBasic;

	public static void main(String[] args) {

		polarityBasic = new PolarityBasic("/home/berthold/workspace/spark-twitter/");
		try {
			polarityBasic.train();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		loadOAuthAccess();

		JavaStreamingContext ssc = new JavaStreamingContext("local[2]", "Twitter-Test", new Duration(1000));

		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc);

		streamContainingSearchTerm(ssc, stream);

	}

	private static void streamContainingSearchTerm(JavaStreamingContext ssc, JavaReceiverInputDStream<Status> stream) {
		JavaDStream<String> statuses = stream.filter(new Function<Status, Boolean>() {

			public Boolean call(Status status) throws Exception {
				if(status.getPlace() != null){
					System.out.println("***Place***" + status.getPlace().getCountryCode());
				}
				if(status.getPlace() != null && "US".equalsIgnoreCase(status.getPlace().getCountryCode())){
					System.out.println(status.getText());
					System.out.println(polarityBasic.mClassifier.classify(status.getText()).bestCategory());
					System.out.println("***********************");
						
				}
				//System.out.println(status.getText());
				//System.out.println(polarityBasic.mClassifier.classify(status.getText()).bestCategory());
				//System.out.println("***********************");
				if (status.getText().contains(SEARCH_TERM)) {
					return true;
				} else {
					return false;
				}
			}
		}).map(new Function<Status, String>() {
			public String call(Status status) {

				return status.getText();
			}
		});
		statuses.window(new Duration(10000), new Duration(1000)).print();
		ssc.checkpoint(HOME_PATH + "/twitter");
		ssc.start();
		System.out.println("Twitter stream started");
	}

	private static void loadOAuthAccess() {
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(HOME_PATH + "twitter.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.getProperties().putAll(properties);
	}
}
