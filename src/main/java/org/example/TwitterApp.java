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

import twitter4j.Status;

public class TwitterApp {

	private static final String HOME_PATH = "/home/berthold/";

	public static void main(String[] args) {

		loadOAuthAccess();
		JavaStreamingContext ssc = new JavaStreamingContext("local[*]", "Twitter-Test", new Duration(60000));

		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc);

		try {
			streamCreatePositiveCaseFiles(ssc, stream);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void streamCreatePositiveCaseFiles(final JavaStreamingContext ssc, JavaDStream<Status> stream)
			throws IOException {

		JavaDStream<String> statuses = stream.filter(new Function<Status, Boolean>() {
			private int count;

			public Boolean call(Status status) throws Exception {
				if (status.getPlace() != null) {
					System.out.println("***Place***" + status.getPlace().getCountryCode());
				}
				String text = status.getText();
				//TODO: collect all positive smiles and check them with UTF codes
				if (status.getPlace() != null && isEnglish(status)
						&& (text.contains(":-)") || text.contains("☺") || text.contains("😍") || text.contains("😅"))) {
					System.out.println(text);
					System.out.println("***********************");
					String message = text.replaceAll(":-\\)", "").replaceAll("☺", "").replaceAll("😍", "")
							.replaceAll("😅", "");
					System.out.println(message);
					return true;
				} else {
					return false;
				}

			}

			private boolean isEnglish(Status status) {
				return "US".equalsIgnoreCase(status.getPlace().getCountryCode())
						|| "GB".equalsIgnoreCase(status.getPlace().getCountryCode());
			}
		}).map(new Function<Status, String>() {
			public String call(Status status) {

				return status.getText();
			}
		});

		statuses.dstream().repartition(1).saveAsTextFiles("/home/berthold/training/pos/train_", "dat");
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
