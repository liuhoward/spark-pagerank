/**
  * Created by Hao Liu on 11/2/16.
  */
import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.util.matching.Regex


object WikipediaPageRank {

  /**
    * parse title from a line
    * @param line string line to be parsed
    * @return title
    */
  def parseTitle(line:String) : Option[String]= {

    // find index of tag <title>
    var start = line.indexOf("<title>");
    if(start == -1) {
      return None;
    }

    // find index of end tag </title>
    var end = line.indexOf("</title>", start);

    if(end == -1) {
      return None;
    }

    // skip tag
    start += 7;
    val title = line.substring(start, end);
    if(title.contains(":")) {
      return None;
    }

    return Some(title);
  }

  /**
    * parse text body from a line
    * @param line given string line
    * @return text body
    */
  def parseBody(line:String) : Option[String] = {

    // find index of tag <text
    var  start0 = line.indexOf("<text");
    if(start0 == -1) {
      return None;
    }
    var start = line.indexOf(">", start0);
    if(start == -1) {
      return None;
    }

    // find index of tag </text>
    var end = line.indexOf("</text>", start);
    if(end == -1) {
      return None;
    }

    // skip ">"
    start += 1;
    val body = line.substring(start, end);
    return Some(body);
  }

  /**
    * main function of PageRank on Spark, written in scala
    * @param args <inputFile> <iteration num>
    */
  def main(args: Array[String]) {

    // check args
    if (args.length != 2) {
      System.err.println(
        "Usage: WikipediaPageRank <inputFile> <iteration>")
      System.exit(-1)
    }

    //create spark configuration
    val sparkConf = new SparkConf()
    sparkConf.setAppName("WikipediaPageRank")

    //inputFile: input wiki text file
    val inputFile = args(0)
    //iterMax: number of update iterations
    val iterMax = args(1).toInt

    //spark context
    val sc = new SparkContext(sparkConf)

    /**
      *  Parse the Wikipedia page
      */
    // input file and filter empty lines
    val input = sc.textFile(inputFile).filter(line => line.length > 1)

    // pattern for title
    //val titlePattern = "<title>.+?</title>".r;
    // pattern for text
    //val textPattern = "<text>.+?</text>".r;

    // pattern for out link
    val linkPattern = "\\[\\[.+?\\]\\]".r;

    // map each line to tuples (title, text)
    val pairs0 = input.map(
      //line => (titlePattern.findFirstIn(line), textPattern.findFirstIn(line))
      line => (parseTitle(line), parseBody(line))
    ).filter{ // filter tuples with empty key or value
      case (x:Option[String], y:Option[String]) => (x.isDefined && y.isDefined)
    };

    // change types from Option[String] to String, and remove useless signs and strings part
    val pairs = pairs0.collect{
      case (Some(x),Some(y)) => //(x.replaceAll("<title>","").replaceAll("</title>", "").replaceAll("\\s", "_"), y.replaceAll("<text>", "").replaceAll("</text>", ""))
        (x.replaceAll("\\s", "_"), y)
    }.filter{ // filter tuples with key containing ":"
      case (x:String, y:String) => (!x.contains(":"))
    };

    // extract out links, and create (link, outlink) tuples, and remove some signs
    val links0 = pairs.flatMap{
      case (x:String, y:String) =>
        linkPattern.findAllIn(y).map{link =>
          val out = link.replace("[[","").replace("]]","").replaceAll("\\s", "_").replaceAll(",", "").replaceAll("&amp;", "&");
          (x, out);
        }
    }

    // filter out some outlinks, group tuples by key (src link)
    val links = links0.filter{
      case (x:String, y:String) => (!y.startsWith("#") && !y.startsWith("&") && !y.startsWith("\'") && !y.startsWith("-") && !y.startsWith("{") && !y.startsWith("|"))
    }.distinct().groupByKey().cache();

    /**
      * initialize ranks for all links
      */
    var ranks = links.mapValues(v => 1.0);
    //println("============================" + input.count() + "========" + pairs0.count() + "===" + pairs.count() + "====" + links0.count() + "===" + links.count() + "=====" + ranks.count())

    /**
      * upate ranks in 10 loops
      */
    for(i <- 1 to iterMax) {
      val contributs = links.join(ranks).values.flatMap{case (link, rank) =>
          val size = link.size;
          link.map(link => (link, rank / size));
      }
      ranks = contributs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _);
    }

    /**
      * sort the ranks and print top 20 of them
      */
    val output = ranks.sortBy(_._2, false).collect()

    var num = output.size;
    /*
    if(num > 20) {
      num = 20;
    }*/
    output.take(num).foreach(tuple => println(tuple._1 + "\t" + tuple._2))

  }

}
