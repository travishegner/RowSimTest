package com.travishegner.RowSimTest

import org.apache.mahout.math.cf.SimilarityAnalysis
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by thegner on 7/9/15.
 */
object RowSimTest extends App{
  val data = Array(
    ("doc1", "tag1"),
    ("doc2", "tag1"),
    ("doc3", "tag1"),
    ("doc3", "tag2"),
    ("doc4", "tag2"),
    ("doc4", "tag5"),
    ("doc5", "tag2"),
    ("doc5", "tag3"),
    ("doc5", "tag4"),
    ("doc5", "tag5")
  )
  val conf = new SparkConf setAppName "RowSimTest"
  val sc = new SparkContext(conf)

  val pdata = sc.parallelize(data)
  val ids = IndexedDatasetSpark(pdata)(sc)

  println(ids.matrix.toString)

  val sims = SimilarityAnalysis.rowSimilarityIDS(ids)

  println(sims.toString)
}
