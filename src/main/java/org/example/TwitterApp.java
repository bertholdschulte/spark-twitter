package org.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;

public class TwitterApp {

	private static final String HOME_PATH = "/home/berthold/";

	private static List<String> positive = new ArrayList<String>();
	private static List<String> negative = new ArrayList<String>();
	private static int records;
	
	private static JavaStreamingContext ssc;
	
	public static void main(String[] args) {
		loadOAuthAccess();
		loadEmoticons();
		
		ssc = new JavaStreamingContext("local[*]", "Twitter-Test", new Duration(60000));

		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc);

		try {
			//streamCreateCaseFiles(ssc, stream, positive,"pos");
			streamCreateCaseFiles(ssc, stream, negative,"neg");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void loadEmoticons() {
		positive.add(":-)");
		positive.add("☺");
		positive.add("😍");
		positive.add("😅");
		negative.add(":-(");
		negative.add("☹");
	}

	private static void streamCreateCaseFiles(final JavaStreamingContext ssc, JavaDStream<Status> stream, List<String> items, String path)
			throws IOException {

		JavaDStream<String> statuses = stream.filter(new Function<Status, Boolean>() {
		

			public Boolean call(Status status) throws Exception {

				String text = status.getText();
				if (items.stream().anyMatch(s->text.contains(s))) {
					records++;
					if (records == 100) {
						System.exit(0);
					}
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

		statuses.dstream().repartition(1).saveAsTextFiles("/home/berthold/training/"+path+"/train_", "dat");
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
