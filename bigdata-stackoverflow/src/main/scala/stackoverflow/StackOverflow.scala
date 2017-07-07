package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag
import org.apache.spark.RangePartitioner
import org.apache.spark.RangePartitioner

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[Int], score: Int, tags: Option[String]) extends Serializable

/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw = rawPostings(lines)
    val grouped = groupedPostings(raw)
    //    
    val scored = scoredPostings(grouped)
    var vectors = vectorPostings(scored)
    //    //assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())
    //
    val means = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
    //    println("Count = " + vectors.count())
    //System.in.read()
  }
}

/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120

  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType = arr(0).toInt,
        id = arr(1).toInt,
        acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
        parentId = if (arr(3) == "") None else Some(arr(3).toInt),
        score = arr(4).toInt,
        tags = if (arr.length >= 6) Some(arr(5).intern()) else None)
    })

  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(Int, Iterable[(Posting, Posting)])] = {
    var questions = postings.filter(_.postingType == 1).map(question => (question.id, question))
    var answers = postings.filter(_.postingType == 2).map(ans => (ans.parentId.get, ans))
    questions.join(answers).groupByKey()
  }

  def getIntFromOption(parentId: Option[Int]): Int = {
    parentId match {
      case Some(s) => s
      case None    => 0
    }
  }

  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(Int, Iterable[(Posting, Posting)])]): RDD[(Posting, Int)] = {

    def answerHighScore(as: Array[Posting]): Int = {
      var highScore = 0
      var i = 0
      while (i < as.length) {
        val score = as(i).score
        if (score > highScore)
          highScore = score
        i += 1
      }
      highScore
    }

    grouped.flatMap(x => x._2).groupByKey().mapValues(v => answerHighScore(v.toArray))

    /* Approach 2 */
    //    val questionScores =
    //      grouped.mapValues(qapairs =>
    //        qapairs.map(
    //          qapair => {
    //            val score = if (qapair._2.score < 0)
    //              0
    //            else
    //              qapair._2.score
    //            (qapair._1, score)
    //          }))
    //    questionScores.mapValues(x => x.reduce((a, b) => (a._1, Math.max(a._2, b._2)))).values
    //    questionScores.map({ case (qid, qscorepairs) => qscorepairs.reduce((x, y) => (x._1, Math.max(x._2, y._2))) })
  }

  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Posting, Int)]): RDD[(Int, Int)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None    => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }
    val questionScorePair = scored.map({
      case (question, maxscore) => (langSpread * getIntFromOption(firstLangInTag(question.tags, langs)), maxscore)
    })
    questionScorePair.persist()
  }

  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(Int, Int)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }

  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    //    val newMeans = means.clone() // you need to compute newMeans

    // RDD(index, point(x,y)) => RDD (index, average(point(x,y) for that index))
    val pointToCurrentCenterIndexMapping = vectors.map(x => (findClosest(x, means), x))
    val currentCenterIndexToPointsMapping = pointToCurrentCenterIndexMapping.groupByKey()
    val currentCenterIndexToNewCenterMapping = currentCenterIndexToPointsMapping.mapValues(averageVectors(_)).collect().sortBy(_._1)
    //    currentCenterIndexToNewCenterMapping.foreach(x => println("currentCenterIndexToNewCenterMapping = " + x))

    val indexedCurrentCenters = means.indices.map(i => (i, means(i))).toMap
    val joinedcurrentCenterIndexToNewCenterMapping = (indexedCurrentCenters ++ currentCenterIndexToNewCenterMapping.toMap).toArray.sortBy(_._1)
    //    joinedcurrentCenterIndexToNewCenterMapping.foreach(x => println("joinedcurrentCenterIndexToNewCenterMapping = " + x))

    val newMeans = joinedcurrentCenterIndexToNewCenterMapping.map(x => x._2)

    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until means.size)
        println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
          f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      println("Reached max iterations!")
      newMeans
    }
  }

  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta

  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while (idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }

  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(Int, Int)]): Array[(String, Double, Int, Int)] = {
    //vectors is langSpread * LanguageIndex, MaxScore)
    val closest = vectors.map(p => (findClosest(p, means), p))
    //    closest.foreach(x => println("closest = " + x))
    val closestGrouped = closest.groupByKey()
    //    val closestGroupedLang = closestGrouped.mapValues(x => x.map(a=>(langs(a._1 / langSpread),a._2)))
    //    val tmp = closestGroupedLang.mapValues(x=> x.groupBy(_._1).mapValues(x => x.size))
    //    tmp.foreach(x => println("Closed Group = " + x) )
    val median = closestGrouped.mapValues { vs =>
      var a = vs.groupBy(_._1).mapValues(x => x.size).reduce((x, y) => if (x._2 > y._2) x; else y)
      val langLabel: String = {
        //   		  println("Language = " + langs(a._1 / langSpread) + " ; Number of hits = " + a._2 + " ; ClusterSize = " + vs.size)
        langs(a._1 / langSpread)
      } // most common language in the cluster 
      val langPercent: Double = {
        1.0 * a._2 / vs.size * 100
      } // percent of the questions in the most common language
      val clusterSize: Int = vs.size
      val medianScore: Int = mymedian(vs.map(x => x._2).toSeq)

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def mymedian(s: Seq[Int]): Int =
    {
      val (lower, upper) = s.sortWith(_ < _).splitAt(s.size / 2)
      if (s.size % 2 == 0) (lower.last + upper.head) / 2 else upper.head
    }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
