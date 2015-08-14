import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.SparkConf

object DFRye {
  def main(args: Array[String]) {    
    val conf = new SparkConf().setAppName("Rye")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._
    
    // Databricks notebook source exported at Fri, 7 Aug 2015 05:52:33 UTC
    val text_urls = Array("https://dl.dropboxusercontent.com/u/105876471/tth.txt", "https://dl.dropboxusercontent.com/u/105876471/coc.txt")
    // val text_urls = Array("https://www.gutenberg.org/cache/epub/77/pg77.txt", "https://www.gutenberg.org/ebooks/2701.txt.utf-8")
    val t1_url = text_urls(0)
    val t2_url = text_urls(1)

    import scala.io.Source
    val t1_tokens = Source.fromURL(t1_url).mkString.split("\\s+")
    val t2_tokens = Source.fromURL(t2_url).mkString.split("\\s+")

    // COMMAND ----------

    val t1_tokensRDD = sc.parallelize(t1_tokens).zipWithIndex()
    val t2_tokensRDD = sc.parallelize(t2_tokens).zipWithIndex()
    println(t1_tokensRDD.take(5))
    println(t2_tokensRDD.take(5).mkString)

    // COMMAND ----------

    val stop_words = Array("a", "i", "an", "as", "able", "about", "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again", "against", "aint", "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "arent", "around", "as", "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "cmon", "cs", "came", "can", "cant", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldnt", "course", "currently", "definitely", "described", "despite", "did", "didnt", "different", "do", "does", "doesnt", "doing", "dont", "done", "down", "downwards", "during", "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "far", "few", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth", "four", "from", "further", "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got", "gotten", "greetings", "had", "hadnt", "happens", "hardly", "has", "hasnt", "have", "havent", "having", "he", "hes", "hello", "help", "hence", "her", "here", "heres", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", "however", "id", "ill", "im", "ive", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isnt", "it", "itd", "itll", "its", "its", "itself", "just", "keep", "keeps", "kept", "know", "known", "knows", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "little", "look", "looking", "looks", "ltd", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not", "nothing", "novel", "now", "nowhere", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "particular", "particularly", "per", "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provides", "que", "quite", "qv", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively", "respectively", "right", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she", "should", "shouldnt", "since", "six", "so", "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup", "sure", "ts", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "thats", "thats", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "theres", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "theyd", "theyll", "theyre", "theyve", "think", "third", "this", "thorough", "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two", "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually", "value", "various", "very", "via", "viz", "vs", "want", "wants", "was", "wasnt", "way", "we", "wed", "well", "were", "weve", "welcome", "well", "went", "were", "werent", "what", "whats", "whatever", "when", "whence", "whenever", "where", "wheres", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whos", "whoever", "whole", "whom", "whose", "why", "will", "willing", "wish", "with", "within", "without", "wont", "wonder", "would", "wouldnt", "yes", "yet", "you", "youd", "youll", "youre", "youve", "your", "yours", "yourself", "yourselves", "zero")

    // COMMAND ----------

    // remove normalize to lowercase, remove stopwords
    val t1_cleaned_tokensRDD = t1_tokensRDD
        .map(p=>(p._1.replaceAll("\\W+", "").toLowerCase, p._2))	
    	.filter(p => { p._1.length>1 && !(stop_words contains p._1) })
    		
    println(t1_cleaned_tokensRDD.take(15).mkString)
    val t2_cleaned_tokensRDD = t2_tokensRDD
        .map(p=>(p._1.replaceAll("\\W+", "").toLowerCase, p._2))	
    	.filter(p => { p._1.length>1 && !(stop_words contains p._1) })

    println(t2_cleaned_tokensRDD.take(15).mkString)

    // COMMAND ----------

    val t1_stemmedRDD = t1_cleaned_tokensRDD.map(p=>(Stemmer.stem(p._1), p._2))
    println(t1_stemmedRDD.take(5).mkString)
    val t2_stemmedRDD = t2_cleaned_tokensRDD.map(p=>(Stemmer.stem(p._1), p._2))


    // COMMAND ----------

    val t1_concRDD = t1_stemmedRDD.groupByKey()
    t1_concRDD.take(5).foreach(ex=> {
    println("key " + ex._1 + " -- loci " + ex._2.mkString(","))
    })
    val t2_concRDD = t2_stemmedRDD.groupByKey()


    // COMMAND ----------
    val t1d1 = t1_concRDD.map(p => (p._1, p._2.toList)).toDF("entry", "loci")
    val t1d2 = t1_concRDD.map(p => (p._1, p._2.toList)).toDF("entry2", "loci2")

    var t1j = t1d1.join(t1d2, t1d1("entry")<t1d2("entry2"))

    val distance = 7
    val findBigramsWithin = (l1: List[Long], l2: List[Long]) => {  
      for { 
        loc1 <- l1
        loc2 <- l2
        if (Math.abs(loc1-loc2)<distance)
      } yield {
        (loc1, loc2)
      }  
    }

    val bigrams = udf(findBigramsWithin)

    val getLen = udf((e:List[Tuple2[Long, Long]]) => e.length)

    t1j = t1j.withColumn("bg", bigrams(t1j("loci"), t1j("loci2")))

    t1j = t1j.filter(getLen(t1j("bg"))>0)
    t1j = t1j.select("entry", "entry2", "bg")
    t1j.show()

    val t2d1 = t2_concRDD.map(p => (p._1, p._2.toList)).toDF("entry", "loci")
    val t2d2 = t2_concRDD.map(p => (p._1, p._2.toList)).toDF("entry2", "loci2")
    var t2j = t2d1.join(t2d2, t2d1("entry")<t2d2("entry2"))
    t2j = t2j.withColumn("bg", bigrams(t2j("loci"), t2j("loci2")))
    t2j = t2j.filter(getLen(t2j("bg"))>0)
    t2j = t2j.select("entry", "entry2", "bg")

    // COMMAND ----------

    val t2rename = t2j.withColumnRenamed("entry","e").withColumnRenamed("entry2","e2").withColumnRenamed("bg", "bg2")
    val joinedDF = t1j.join(t2rename, t1j("entry") === t2rename("e") && t1j("entry2") === t2rename("e2")).select("entry", "entry2", "bg", "bg2")
    joinedDF.show()

    // COMMAND ----------

    joinedDF.explain
  }
}
    // COMMAND ----------