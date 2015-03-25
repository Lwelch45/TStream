package com.laurence.tstream;

import com.laurence.tstream.util.Util;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
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
import scala.Tuple3;
import scala.Tuple4;
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

    final StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

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
        return new Tuple2<Status, String>(statusStringTuple2._1(), txt);
      }
    });

    //score the tweets
    JavaDStream<Tuple3<Status, String, Integer>> scoredMessages = filteredMessages.map(
    new Function<Tuple2<Status, String>, Tuple3<Status, String, Integer>>() {
      @Override
      public Tuple3<Status, String, Integer> call(Tuple2<Status, String> statusStringTuple2) throws Exception {

        int mainSentiment = 0;
        if (statusStringTuple2._2() != null && statusStringTuple2._2().length() > 0) {
          int longest = 0;
          Annotation annotation = pipeline.process(statusStringTuple2._2());
          for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
            String partText = sentence.toString();
            if (partText.length() > longest) {
              mainSentiment = sentiment;
              longest = partText.length();
            }

          }
        }
        return new Tuple3<Status, String, Integer>
        (statusStringTuple2._1(), statusStringTuple2._2(), mainSentiment);
      }
    });

    // output decision
    JavaDStream<Tuple4<Status, String, Integer, String>> res = scoredMessages.map(
      new Function<Tuple3<Status, String, Integer>, Tuple4<Status, String, Integer, String>>() {
      @Override
      public Tuple4<Status, String, Integer, String> call(Tuple3<Status, String, Integer> tpl) throws Exception {
        return new Tuple4<Status, String, Integer, String>
          (tpl._1(),tpl._2(),tpl._3(), toStatus(tpl._3()));
      }
    });



    res.foreachRDD(new Function<JavaRDD<Tuple4<Status, String, Integer, String>>, Void>() {
      @Override
      public Void call(JavaRDD<Tuple4<Status, String, Integer, String>> RDDtpl) throws Exception {
        RDDtpl.foreach(new VoidFunction<Tuple4<Status, String, Integer, String>>() {
          @Override
          public void call(Tuple4<Status, String, Integer, String> tpl) throws Exception {
            HttpClient client = new DefaultHttpClient();
            HttpPost post = new HttpPost("http://laurencewelch.science:3000/post");
            String content = String.format(
            "{\"tweet\": \"%s\", " +
            "\"text\": \"%s\", " +
            "\"score\": \"%f\", " +
            "\"label\": \"%s\" }",
            DataObjectFactory.getRawJSON(tpl._1()).toString(),
            tpl._2(),
            tpl._3(),
            tpl._4());
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
