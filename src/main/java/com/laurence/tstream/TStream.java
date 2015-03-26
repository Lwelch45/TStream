package com.laurence.tstream;

import com.laurence.tstream.util.Util;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;
import twitter4j.Status;
import twitter4j.json.DataObjectFactory;

import java.util.List;
import java.util.Properties;

/**
 * Created by laurencewelch on 3/24/15.
 */
public class TStream {

  private final Logger Log = Logger.getLogger(TStream.class);

  private static final List<String> stopWords = new Util().readLines("/stop-words.txt");
  private static final List<String> positiveWords = new Util().readLines("/positive-words.txt");
  private static final List<String> negativeWords = new Util().readLines("/negative-words.txt");

  private static final int MAX_LIST_SIZE = 50000;
  private static Properties props = new Properties();

  private static String toStatus(int sentiment) {
    switch (sentiment) {
      case 0:
        return "negative";
      case 1:
        return "semi negative";
      case 2:
        return "netural";
      case 3:
        return "semi positive";
      case 4:
        return "positive";
      default:
        return "";
    }
  }

  public static void main(String[] args) throws Exception {
    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");

    BasicConfigurator.configure();
    SparkConf configuration = new SparkConf().setAppName("Twitter Stream Sentiment Analysis");

    configuration.setMaster("local[2]");

    Util.configureTwitterCredentials("/twitter.config");

    JavaStreamingContext jsc = new JavaStreamingContext(configuration, Durations.seconds(2L));

    JavaReceiverInputDStream<Status> messages = TwitterUtils.createStream(jsc);

    //add mutable field to stream that holds messages
    JavaDStream<Tuple2<Status,String>> textMessages =  messages.map(
      new Function<Status, Tuple2<Status,String>>() {
      @Override
      public Tuple2<Status,String> call(Status status) throws Exception {
        return new Tuple2<Status, String>(status, status.getText().replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase());
      }
    });

    //filter out all of the stop words
    JavaDStream<Tuple2<Status, String>> filteredMessages = textMessages.map(
      new Function<Tuple2<Status, String>, Tuple2<Status, String>>() {
      @Override
      public Tuple2<Status, String> call(Tuple2<Status, String> statusStringTuple2) throws Exception {
        String txt = statusStringTuple2._2();
        for (String stopWord : stopWords){
          txt = txt.replaceAll("\\b" + stopWord + "\\b", "");
        }
        return new Tuple2<>(statusStringTuple2._1(), txt);
      }
    });

//score the tweets
    JavaDStream<Tuple4<Status, String, Double, Double>> scoredMessages = filteredMessages.map(
    new Function<Tuple2<Status, String>, Tuple4<Status, String, Double, Double>>() {
      @Override
      public Tuple4<Status, String, Double, Double> call(Tuple2<Status, String> statusStringTuple2) throws Exception {
        String[] words = statusStringTuple2._2().split(" ");
        double length = words.length;
        double positive = 0;
        double negative = 0;
        for (int i = words.length -1; i >= 0; i--)
        {
          if (positiveWords.contains(words[i])) {
            positive++;

            // naive assumption that the word that proceeds a positve word is also positive.
             if ((i < words.length -1) && (positiveWords.size() < MAX_LIST_SIZE) && !(positiveWords.contains(words[i+ 1]))){
              positiveWords.add(words[i+1]);
            }
          }
          if (negativeWords.contains(words[i])){
            negative++;
            // naive assumption that the word that proceeds a positve word is also positive.
            if ((i < words.length -1) && (negativeWords.size() < MAX_LIST_SIZE) && !(negativeWords.contains(words[i+ 1]))){
              negativeWords.add(words[i+1]);
            }
          }
        }
        return new Tuple4<>
        (statusStringTuple2._1(), statusStringTuple2._2(), (positive/length), (negative/length));
      }
    });

    // output decision
    JavaDStream<Tuple5<Status, String, Double, Double, String>> res = scoredMessages.map(
    new Function<Tuple4<Status, String, Double, Double>, Tuple5<Status, String, Double, Double, String>>() {
      @Override
      public Tuple5<Status, String, Double, Double, String> call(Tuple4<Status, String, Double, Double> tpl) throws Exception {
        if ((tpl._3() == 0) && (tpl._4() == 0)){
          return new Tuple5<>
          (tpl._1(),tpl._2(),tpl._3(),tpl._4(), "neutral" );
        }
        return new Tuple5<>
        (tpl._1(),tpl._2(),tpl._3(),tpl._4(), (tpl._3() > tpl._4()) ? "positive" : "negative" );
      }
    });

    res.foreachRDD(new Function<JavaRDD<Tuple5<Status, String, Double, Double, String>>, Void>() {
      @Override
      public Void call(JavaRDD<Tuple5<Status, String, Double, Double, String>> RDDtpl) throws Exception {
        RDDtpl.foreach(new VoidFunction<Tuple5<Status, String, Double, Double, String>>() {
          @Override
          public void call(Tuple5<Status, String, Double, Double, String> tpl) throws Exception {
            HttpClient client = new DefaultHttpClient();
            HttpPost post = new HttpPost("http://laurencewelch.science:3000/post");
            String content = String.format(
            "{\"tweet\": \"%s\", " +
            "\"text\": \"%s\", " +
            "\"pos\": \"%f\", " +
            "\"neg\": \"%f\", " +
            "\"score\": \"%s\" }",
            DataObjectFactory.getRawJSON(tpl._1()),
            tpl._2(),
            tpl._3(),
            tpl._4(),
            tpl._5());
            try
            {
              post.setEntity(new StringEntity(content));
              HttpResponse response = client.execute(post);
              org.apache.http.util.EntityUtils.consume(response.getEntity());
            }
            catch (Exception ex)
            {
              Logger LOG = Logger.getLogger(this.getClass());
              LOG.error("exception thrown while attempting to post", ex);
              LOG.trace(null, ex);
            }
          }
        });
        return null;
      }
    });

    jsc.start();
    jsc.awaitTermination();
  }
}
