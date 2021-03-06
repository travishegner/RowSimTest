package com.travishegner.RowSimTest

import java.io.{BufferedWriter, FileWriter, File}

import org.apache.mahout.math.cf.SimilarityAnalysis
import org.apache.mahout.math.indexeddataset.{IndexedDataset, IndexedDatasetWriteBooleanSchema}
import org.apache.mahout.sparkbindings.SparkDistributedContext
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.mahout.drivers.TextDelimitedIndexedDatasetWriter

/**
 * Created by thegner on 7/9/15.
 */
object RowSimTest extends App{
  val conf = (new SparkConf)
    .setAppName("RowSimTest")
    .set("spark.hadoop.validateOutputSpecs", "false")
//    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    .set("spark.kryo.registrator", "org.apache.mahout.sparkbindings.io.MahoutKryoRegistrator")
//    .set("spark.kryo.referenceTracking", "false")
//    .set("spark.kryoserializer.buffer.mb", "300")
//    .set("spark.executor.memory", "4g")

  val sc = new SparkContext(conf)

  val data = Array(
("284198","5A8DD36FD8A9CDFFA56AB5C34B4477FB"),
("277410","3619FF503CBB824294A39FED29E08CD0"),
("272934","5A8DD36FD8A9CDFFA56AB5C34B4477FB"),
("281966","206270CAF28AB379F87C2F741AA6FAEC"),
("277342","3619FF503CBB824294A39FED29E08CD0"),
("278488","B77DE1E594AB934CDF111432F07E0E38"),
("278488","7516132C51E75CE65534CBDCC96C2C45"),
("278488","A398735204C0606927167B4E55B54C66"),
("278488","B72ED67875ADC71A1B2024B7158FE9F5"),
("278488","47BBE2ADBE0AC688D540D046086BB820"),
("278488","0A262C926F5CA2F6B5DA4309ECC664E1"),
("278488","889247FBCE7EC9B412AB96DDBB3A10EF"),
("278488","DC6E82EEAC30A649B704798D093C1D2E"),
("278488","D825D16C0A9924DB4CA7D31E3B63BAB9"),
("277342","3619FF503CBB824294A39FED29E08CD0"),
("278488","B77DE1E594AB934CDF111432F07E0E38"),
("278488","7516132C51E75CE65534CBDCC96C2C45"),
    ("278488","A398735204C0606927167B4E55B54C66"),
    ("278488","B72ED67875ADC71A1B2024B7158FE9F5"),
    ("278488","47BBE2ADBE0AC688D540D046086BB820"),
    ("278488","0A262C926F5CA2F6B5DA4309ECC664E1"),
    ("278488","889247FBCE7EC9B412AB96DDBB3A10EF"),
    ("278488","DC6E82EEAC30A649B704798D093C1D2E"),
    ("278488","D825D16C0A9924DB4CA7D31E3B63BAB9"),
    ("278488","48FAE0CA541CC3FD2701793130830160"),
    ("278488","908AC36A5A8F7DD6136FD407BE1D6BE2"),
    ("278488","48A2C743118C1B16327DF97BB04EB4AA"),
    ("278488","C72D81070A9B76F6C90CDA066C61BA33"),
    ("278488","3F0960DF87759CBC29EC534061DEE852"),
    ("278488","6020B8A5A4200A83284E68DB6EC14A23"),
    ("278488","7B729D8B0881AE3479CE3C741D689270"),
    ("278488","478EDAF0EE6D381CFB61B6FC1DA1977D"),
    ("278488","C77151C16414B2EF0DD2B8CEB7180CF5"),
    ("277338","3619FF503CBB824294A39FED29E08CD0"),
    ("288166","A745F2EB7224EB330DFC0F2D8D3C39D5"),
    ("277316","3619FF503CBB824294A39FED29E08CD0"),
    ("298416","8F192D90F568DE1BE9FD82DD0DBCAB81"),
    ("271942","5A8DD36FD8A9CDFFA56AB5C34B4477FB"),
    ("288732","A745F2EB7224EB330DFC0F2D8D3C39D5"),
    ("277418","3619FF503CBB824294A39FED29E08CD0"),
    ("277364","3619FF503CBB824294A39FED29E08CD0"),
    ("298162","44FF4A553E30D7E8FB7EBAD2F90C776E"),
    ("280332","3619FF503CBB824294A39FED29E08CD0"),
    ("290846","EAE1149AE7FD3CB5C95F726CC2B5BE02"),
    ("287756","6F7A108F83995CFAABA1A46DFEED6540"),
    ("288730","A745F2EB7224EB330DFC0F2D8D3C39D5"),
    ("297610","206270CAF28AB379F87C2F741AA6FAEC"),
    ("297894","EE06E6F6B9C2B37710FBE248F9F4762E"),
    ("272692","3619FF503CBB824294A39FED29E08CD0"),
("278488","48FAE0CA541CC3FD2701793130830160"),
("278488","908AC36A5A8F7DD6136FD407BE1D6BE2"),
("278488","48A2C743118C1B16327DF97BB04EB4AA"),
("278488","C72D81070A9B76F6C90CDA066C61BA33"),
("278488","3F0960DF87759CBC29EC534061DEE852"),
("278488","6020B8A5A4200A83284E68DB6EC14A23"),
("278488","7B729D8B0881AE3479CE3C741D689270"),
("278488","478EDAF0EE6D381CFB61B6FC1DA1977D"),
("278488","C77151C16414B2EF0DD2B8CEB7180CF5"),
("277338","3619FF503CBB824294A39FED29E08CD0"),
("288166","A745F2EB7224EB330DFC0F2D8D3C39D5"),
("277316","3619FF503CBB824294A39FED29E08CD0"),
("298416","8F192D90F568DE1BE9FD82DD0DBCAB81"),
("271942","5A8DD36FD8A9CDFFA56AB5C34B4477FB"),
("288732","A745F2EB7224EB330DFC0F2D8D3C39D5"),
("277418","3619FF503CBB824294A39FED29E08CD0"),
("277364","3619FF503CBB824294A39FED29E08CD0"),
("298162","44FF4A553E30D7E8FB7EBAD2F90C776E"),
("280332","3619FF503CBB824294A39FED29E08CD0"),
("290846","EAE1149AE7FD3CB5C95F726CC2B5BE02"),
("287756","6F7A108F83995CFAABA1A46DFEED6540"),
("288730","A745F2EB7224EB330DFC0F2D8D3C39D5"),
("297610","206270CAF28AB379F87C2F741AA6FAEC"),
("297894","EE06E6F6B9C2B37710FBE248F9F4762E"),
("272692","3619FF503CBB824294A39FED29E08CD0"),
("296176","206270CAF28AB379F87C2F741AA6FAEC"),
("288424","6F7A108F83995CFAABA1A46DFEED6540"),
("271944","5A8DD36FD8A9CDFFA56AB5C34B4477FB"),
("278900","5A8DD36FD8A9CDFFA56AB5C34B4477FB"),
("277388","3619FF503CBB824294A39FED29E08CD0"),
("298374","AE94BE3CD532CE4A025884819EB08C98"),
    ("298374","AE94BE3CD532CE4A025884819EB08C98"),
    ("278490","B77DE1E594AB934CDF111432F07E0E38"),
    ("278490","7516132C51E75CE65534CBDCC96C2C45"),
    ("278490","A398735204C0606927167B4E55B54C66"),
    ("278490","B72ED67875ADC71A1B2024B7158FE9F5"),
    ("278490","47BBE2ADBE0AC688D540D046086BB820"),
    ("278490","0A262C926F5CA2F6B5DA4309ECC664E1"),
    ("278490","889247FBCE7EC9B412AB96DDBB3A10EF"),
    ("278490","DC6E82EEAC30A649B704798D093C1D2E"),
    ("278490","D825D16C0A9924DB4CA7D31E3B63BAB9"),
    ("278490","48FAE0CA541CC3FD2701793130830160"),
    ("278490","908AC36A5A8F7DD6136FD407BE1D6BE2"),
("278490","B77DE1E594AB934CDF111432F07E0E38"),
("278490","7516132C51E75CE65534CBDCC96C2C45"),
("278490","A398735204C0606927167B4E55B54C66"),
("278490","B72ED67875ADC71A1B2024B7158FE9F5"),
("278490","47BBE2ADBE0AC688D540D046086BB820"),
("278490","0A262C926F5CA2F6B5DA4309ECC664E1"),
("278490","889247FBCE7EC9B412AB96DDBB3A10EF"),
("278490","DC6E82EEAC30A649B704798D093C1D2E"),
("278490","D825D16C0A9924DB4CA7D31E3B63BAB9"),
("278490","48FAE0CA541CC3FD2701793130830160"),
("278490","908AC36A5A8F7DD6136FD407BE1D6BE2"),
("278490","48A2C743118C1B16327DF97BB04EB4AA"),
("278490","C72D81070A9B76F6C90CDA066C61BA33"),
("278490","3F0960DF87759CBC29EC534061DEE852"),
("278490","6020B8A5A4200A83284E68DB6EC14A23"),
("278490","7B729D8B0881AE3479CE3C741D689270"),
("278490","478EDAF0EE6D381CFB61B6FC1DA1977D"),
("290665","EE06E6F6B9C2B37710FBE248F9F4762E"),
("290665","206270CAF28AB379F87C2F741AA6FAEC"),
("275033","3619FF503CBB824294A39FED29E08CD0"),
("290509","EE06E6F6B9C2B37710FBE248F9F4762E"),
("290667","6F7A108F83995CFAABA1A46DFEED6540"),
("274991","3619FF503CBB824294A39FED29E08CD0"),
("288169","A745F2EB7224EB330DFC0F2D8D3C39D5"),
("274885","3619FF503CBB824294A39FED29E08CD0"),
("275441","5A8DD36FD8A9CDFFA56AB5C34B4477FB"),
("295585","1FF9A9643A89EA7D80D86E5547E9EA56"),
("295585","C778E3589891F914E03D05C285BC57B8"),
("289149","6F7A108F83995CFAABA1A46DFEED6540"),
("289149","3F5B9A17314E241A8FAD421D20F1CC8A"),
("281751","5A8DD36FD8A9CDFFA56AB5C34B4477FB"),
("280333","3619FF503CBB824294A39FED29E08CD0"),
("277317","3619FF503CBB824294A39FED29E08CD0"),
("288731","A745F2EB7224EB330DFC0F2D8D3C39D5"),
("274977","3619FF503CBB824294A39FED29E08CD0"),
("272935","5A8DD36FD8A9CDFFA56AB5C34B4477FB"),
("288733","A745F2EB7224EB330DFC0F2D8D3C39D5"),
("277323","3619FF503CBB824294A39FED29E08CD0"),
("285371","3F5B9A17314E241A8FAD421D20F1CC8A"),
("288237","A745F2EB7224EB330DFC0F2D8D3C39D5"),
("288167","A745F2EB7224EB330DFC0F2D8D3C39D5"),
("277337","3619FF503CBB824294A39FED29E08CD0"),
("292989","206270CAF28AB379F87C2F741AA6FAEC"),
("288163","A745F2EB7224EB330DFC0F2D8D3C39D5"),
("288423","6F7A108F83995CFAABA1A46DFEED6540"),
("277935","EE06E6F6B9C2B37710FBE248F9F4762E"),
("287753","6F7A108F83995CFAABA1A46DFEED6540"),
("297981","1C359E8339D4BCC3EA24058688A7D432"),
("272415","48FAE0CA541CC3FD2701793130830160"),
("272415","908AC36A5A8F7DD6136FD407BE1D6BE2"),
("272415","48A2C743118C1B16327DF97BB04EB4AA"),
("272415","C72D81070A9B76F6C90CDA066C61BA33"),
("272415","3F0960DF87759CBC29EC534061DEE852"),
("272415","6020B8A5A4200A83284E68DB6EC14A23"),
("272415","7B729D8B0881AE3479CE3C741D689270"),
("272415","478EDAF0EE6D381CFB61B6FC1DA1977D"),
("272415","C77151C16414B2EF0DD2B8CEB7180CF5"),
("272415","B77DE1E594AB934CDF111432F07E0E38"),
("272415","7516132C51E75CE65534CBDCC96C2C45"),
("272415","A398735204C0606927167B4E55B54C66"),
("272415","B72ED67875ADC71A1B2024B7158FE9F5"),
("272415","47BBE2ADBE0AC688D540D046086BB820"),
("272415","0A262C926F5CA2F6B5DA4309ECC664E1"),
("272415","889247FBCE7EC9B412AB96DDBB3A10EF"),
("272415","DC6E82EEAC30A649B704798D093C1D2E"),
("272415","D825D16C0A9924DB4CA7D31E3B63BAB9"),
("271943","5A8DD36FD8A9CDFFA56AB5C34B4477FB"),
("293957","206270CAF28AB379F87C2F741AA6FAEC"),
("294275","206270CAF28AB379F87C2F741AA6FAEC"),
("284211","5A8DD36FD8A9CDFFA56AB5C34B4477FB"),
("277313","3619FF503CBB824294A39FED29E08CD0"),
("277367","3619FF503CBB824294A39FED29E08CD0"),
("274883","3619FF503CBB824294A39FED29E08CD0"),
("288007","A61913C8427259A367F01FDE24CAFA56"),
("288007","B1E4C6F69206683477D1D46275F56C81"),
("288007","EE06E6F6B9C2B37710FBE248F9F4762E"),
("288007","4577446B5F3A031FD2DE8B39B770D4A6")
 )

  val pdata = sc.parallelize(data)

  //This block fails sporadically
  val ids = IndexedDatasetSpark(pdata)(sc)
  //val ids = IndexedDatasetSpark(pdata map (r => (r._2, r._1)))(sc)

  val sims = ids.create(SimilarityAnalysis.rowSimilarity(ids.matrix, 0xdeadbeef, Int.MaxValue, Int.MaxValue), ids.rowIDs, ids.rowIDs)
  sims.dfsWrite("sims.txt", IndexedDatasetWriteBooleanSchema)(new SparkDistributedContext(sc))
}
