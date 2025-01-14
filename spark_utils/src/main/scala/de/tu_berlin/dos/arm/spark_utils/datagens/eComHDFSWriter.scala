package de.tu_berlin.dos.arm.spark_utils.datagens

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source

object eComHDFSWriter {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      Console.err.println("Usage: eComHDFSWriter <filePath> <outputPath> <defaultHdfsFs>")
      System.exit(-1)
    }

    println("Start writing eCom data (for Analytics) on Hadoop...")

    val filePath = args(0)
    val outputPath = args(1)
    val defaultFs = args(2)


    System.setProperty("HADOOP_USER_NAME", "drms")
    val path = new Path(outputPath)
    val conf = new Configuration()
    conf.set("fs.defaultFS", defaultFs)
    //conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName);
    //conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName);
    val fs = FileSystem.get(conf)
    val os = fs.create(path)
    // get the content of the file
    val bufferedSource = Source.fromFile(filePath)
    for (line <- bufferedSource.getLines) {
      // seperate each line by linebreak
      os.writeBytes(line.mkString + "\n")
    }

    bufferedSource.close
    fs.close()

    println("Finished writing eCom data (for Analytics) on Hadoop...")
  }

}