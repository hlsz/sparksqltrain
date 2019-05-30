package com.data.demo

import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper}
import org.apache.hadoop.mapreduce.lib.output.{MultipleOutputs, TextOutputFormat}
import org.apache.hadoop.util.GenericOptionsParser

class SplitMapper extends Mapper[Object, Text, Text, Text]{

  private var multipleOutputs:MultipleOutputs[Text, Text] = null

  override protected def setup(context: Mapper[Object, Text, Text, Text]#Context): Unit ={
//    pattern = Pattern.compile("http://([^/]+).+S")
    multipleOutputs = new MultipleOutputs(context)
    println("setup ok...")
  }

  override
  def map(key:Object, value: Text, context:Mapper[Object, Text, Text, Text]#Context): Unit = {
    var keys =  "26/Jan/2015,27/Jan/2015".split(",")

    val word = new Text;

    var line = value.toString();

    for(key <- keys) {
      word.set(key)
      if (line.contains(key)) {
        var mkey :String = key.toString().replace("/", "")
        context.write(word, value)
        multipleOutputs.write(mkey,
          word,
          value);

      }
    }
  }

  override
  protected def cleanup(context: Mapper[Object, Text, Text, Text]#Context): Unit = {
    multipleOutputs.close()
  }

}

object SplitFile {

  def main(args: Array[String]): Unit = {
    val conf  = new Configuration()
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs()
    var usage =
      """
        |Usage: FileSplit <inputdir> <outdir> <tmpdir> <keeyoriginfile>[<key1> <key2> <key3> key4>...]
        |inputdir is a file or dir
        |outputdir is a dir need not blank
        |tmpdir use for tmp
        |keeyoriginfile is a bool
      """

    print("\n\n")
    print(args.toString())
    print(otherArgs)
    println()
    if (args.length < 5){
      print(usage)
      return 2
    }

    val job = new Job(conf, "com.data.demo.SplitFile")

    job.setJarByClass(classOf[SplitMapper])
    job.setMapperClass(classOf[SplitMapper])

    job.setOutputKeyClass(classOf[Text])
    job.setInputFormatClass(classOf[TextInputFormat])

    var inputdir = new Path(otherArgs(0))
    var outputdir = new Path(otherArgs(1))
    var tmpdir = new Path(otherArgs(2))
    var keyoriginfile :String = otherArgs(3)
    var splitkeys:Array[String] = otherArgs.slice(4, otherArgs.length)
    var keys = splitkeys.mkString(",")
    var test_str:String =
      """
        |input: %s
        |output: %s
        |tmpdir: %s
        |keys: %s
      """.format(inputdir, outputdir, tmpdir, keys)
    print(test_str);

    print(splitkeys);

    for(key <- splitkeys) {
      var mkey:String = key.toString().replace("/","")
      print(mkey)

      MultipleOutputs.addNamedOutput(job, mkey,
        classOf[TextOutputFormat[Text, Text]],
        classOf[Text],
        classOf[Text])
    }

    job.setNumReduceTasks(0)

    job.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job, inputdir)
//    FileOutputFormat.setOutputPath(job, tmpdir)

//    job.setOutputFormatClass(classOf[MyMultipleOutputFormat])
//    job.setOutputFormatClass(classOf[CustomMultipleTextOutputFormat])


    if(job.waitForCompletion(true)) 0 else 1


  }

}
