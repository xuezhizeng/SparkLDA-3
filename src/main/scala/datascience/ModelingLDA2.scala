package datascience

import edu.stanford.nlp.process.Morphology
import edu.stanford.nlp.simple.Document
import main.{MainClass, RunJob}
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.{Level, Logger}
import scala.collection.JavaConversions._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, EMLDAOptimizer, LDA, OnlineLDAOptimizer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}


/**
  * Created by gportier on 26/01/2018.
  */

case class Params(
                   input: String = "",
                   k: Int = 7,
                   maxIterations: Int = 50,
                   docConcentration: Double = -1,
                   topicConcentration: Double = -1,
                   vocabSize: Int = 2900000,
                   stopwordFile: String = "src/main/resources/stopWords.txt",
                   algorithm: String = "em",
                   checkpointDir: Option[String] = None,
                   checkpointInterval: Int = 10)


class ModelingLD2App() extends RunJob {

  private val sc = MainClass.sc
  private val spark: SparkSession = MainClass.spark
  private val PROP: PropertiesConfiguration = MainClass.PROP

  def run() {

    val lda = new ModelingLDA2(sc,spark)
    val defaultParams = Params().copy(input = "src/main/resources/docs/Transformed/*/*")
    lda.run(defaultParams)

  }

}

class ModelingLDA2(sc: SparkContext, spark: SparkSession) {

  def run(params: Params): Unit = {

    val preprocessStart = System.nanoTime()
    val (corpus, vocabArray, actualNumTokens) =
      preprocess(sc, params.input, params.vocabSize, params.stopwordFile)
    val actualCorpusSize = corpus.count()
    val actualVocabSize = vocabArray.length
    val preprocessElapsed = (System.nanoTime() - preprocessStart) / 1e9
    corpus.cache()
    println()
    println(s"Corpus summary:")
    println(s"\t Training set size: $actualCorpusSize documents")
    println(s"\t Vocabulary size: $actualVocabSize terms")
    println(s"\t Training set size: $actualNumTokens tokens")
    println(s"\t Preprocessing time: $preprocessElapsed sec")
    println()

    // Run LDA.
    val lda = new LDA()

    val optimizer = params.algorithm.toLowerCase match {
      case "em" => new EMLDAOptimizer
      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
      case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / actualCorpusSize)
      case _ => throw new IllegalArgumentException(
        s"Only em, online are supported but got ${params.algorithm}.")
    }

    lda.setOptimizer(optimizer)
      .setK(params.k)
      .setMaxIterations(params.maxIterations)
      .setDocConcentration(params.docConcentration)
      .setTopicConcentration(params.topicConcentration)
      .setCheckpointInterval(params.checkpointInterval)
    if (params.checkpointDir.nonEmpty) {
      sc.setCheckpointDir(params.checkpointDir.get)
    }
    val startTime = System.nanoTime()
    val ldaModel = lda.run(corpus)
    val elapsed = (System.nanoTime() - startTime) / 1e9

    println(s"Finished training LDA model.  Summary:")
    println(s"\t Training time: $elapsed sec")



    if (ldaModel.isInstanceOf[DistributedLDAModel]) {
      val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
      val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble
      println(s"\t Training data average log likelihood: $avgLogLikelihood")
      println()
      val localLDAModel = distLDAModel.toLocal
      val avglogperplexity = localLDAModel.logPerplexity(corpus) / actualCorpusSize.toDouble
      println(s"\t Training data average log Perplexity: $avglogperplexity")
      println()

      //create test input, convert to term count, and get its topic distribution
      val test_input = "" // TODO Add path
      val (test_document, _, _) =
        preprocess(sc, test_input, params.vocabSize, params.stopwordFile)

      val topicDistributions = localLDAModel.topicDistributions(test_document)
      println("first topic distribution:" + topicDistributions.first._2.toArray.mkString(", "))


    }

    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 20)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }
    println(s"${params.k} topics:")
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (term, weight) =>
        println(s"$term\t$weight")
      }
      println()
    }

    var topicLabels: Set[String] = Set()

    var labellist = ""
    topicIndices.foreach { case (terms, termWeights) =>
      var i = 0
      while (i < 20 && topicLabels.contains(vocabArray(terms(i)))) {
        i = i + 1;
      }
      labellist += vocabArray(terms(i)) + ","
      topicLabels += vocabArray(terms(i))
    }

    var distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]


  }

  def preprocess(
                  sc: SparkContext,
                  paths: String,
                  vocabSize: Int,
                  stopwordFile: String): (RDD[(Long, Vector)], Array[String], Long) = {

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    import spark.implicits._
    //Reading the Whole Text Files
    val initialrdd_0 = spark.sparkContext.wholeTextFiles(paths)
    //neinitialrdd_0.take(2).foreach(println)
    val doc_path_index = initialrdd_0.map(_._1)
      .map(_.stripPrefix("file:/")) // TODO : Path
      .map(_.stripSuffix(".txt"))
      .zipWithIndex()
    //doc_path_index.saveAsTextFile(s"doc_file_index")
    //doc_path_index.take(2).foreach(println)
    val initialrdd = initialrdd_0.map(_._2)
    initialrdd.cache()
    val rdd = initialrdd.mapPartitions { partition =>
      val morphology = new Morphology()
      partition.map { value =>
        LDAHelper.getLemmaText(value, morphology)
      }
    }.map(LDAHelper.filterSpecialCharacters)
    rdd.cache()
    initialrdd.unpersist()
    val df = rdd.toDF("docs")
    val customizedStopWords: Array[String] = if (stopwordFile.isEmpty) {
      Array.empty[String]
    } else {
      val stopWordText = sc.textFile(stopwordFile).collect()
      stopWordText.flatMap(_.stripMargin.split(","))
    }
    //Tokenizing using the RegexTokenizer
    val tokenizer = new RegexTokenizer().setInputCol("docs").setOutputCol("rawTokens")

    //Removing the Stop-words using the Stop Words remover
    val stopWordsRemover = new StopWordsRemover().setInputCol("rawTokens").setOutputCol("tokens")
    stopWordsRemover.setStopWords(stopWordsRemover.getStopWords ++ customizedStopWords)

    //Converting the Tokens into the CountVector
    val countVectorizer = new CountVectorizer().setVocabSize(vocabSize).setInputCol("tokens").setOutputCol("features")

    //Setting up the pipeline
    val pipeline = new Pipeline().setStages(Array(tokenizer, stopWordsRemover, countVectorizer))

    val model = pipeline.fit(df)
    val documents = model.transform(df).select("features").rdd.map {
      case Row(features: MLVector) => Vectors.fromML(features)
    }.zipWithIndex().map(_.swap)

    //documents.take(2).foreach(println)

    (documents,
      model.stages(2).asInstanceOf[CountVectorizerModel].vocabulary, // vocabulary
      documents.map(_._2.numActives).sum().toLong) // total token count
  }
}



object LDAHelper {

  def filterSpecialCharacters(document: String) = document.replaceAll("""[! @ # $ % ^ & * ( ) _ + - − , " ' ; : . ` ? --]""", " ")

  def getStemmedText(document: String) = {
    val morphology = new Morphology()
    new Document(document).sentences().toList.flatMap(_.words().toList.map(morphology.stem)).mkString(" ")
  }

  def getLemmaText(document: String, morphology: Morphology) = {
    val string = new StringBuilder()
    val value = new Document(document).sentences().toList.flatMap { a =>
      val words = a.words().toList
      val tags = a.posTags().toList
      (words zip tags).toMap.map { a =>
        val newWord = morphology.lemma(a._1, a._2)
        val addedWoed = if (newWord.length > 3) {
          newWord
        } else {
          ""
        }
        string.append(addedWoed + " ")
      }
    }
    string.toString()
  }
}

