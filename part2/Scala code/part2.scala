import java.lang.Iterable

import java.util.StringTokenizer

import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.Path

import org.apache.hadoop.io.{FloatWritable, Text}

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.mutable.TreeMap

import scala.collection.mutable.ArrayBuffer

import scala.collection.JavaConverters._

object Main{
	class TokenizerMapper extends Mapper[Object, Text, Text, FloatWritable]{
		val mm: TreeMap[String,Float] = new TreeMap()
		val num = new FloatWritable()
		val word = new Text()
		override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, FloatWritable]#Context): Unit = {
			val line: String = value.toString
			val arr = line.split(",")
			val date = (arr(0).split(" "))(0).split("-")
			val month = date(1)
			val pickLat = ((arr(3).toDouble*1000).round /1000.toDouble).toString
			val pickLon= ((arr(4).toDouble*1000).round /1000.toDouble).toString
			val dropLat = ((arr(5).toDouble*1000).round /1000.toDouble).toString
			val dropLon= ((arr(6).toDouble*1000).round /1000.toDouble).toString
			val fare = arr(1).toFloat
			if(pickLat.toFloat!=0.0 && pickLon.toFloat!=0.0 && dropLat.toFloat!=0.0 && dropLon.toFloat!=0.0)
			{
				val k: String  = month+","+pickLat+","+pickLon+","+dropLat+","+dropLon
				val foo = mm.get(k)
				foo match{
					case Some(x) =>	
						if(x<fare)
						{
							mm.put (k,fare)
						}
					case None =>
						mm.put(k,fare)
				}
			}
		}

		override def cleanup(context:Mapper[Object, Text, Text, FloatWritable]#Context) : Unit ={
			val lst = mm.toList
			for( (x,y) <- lst){
				word.set(x)
				num.set(y)
				context.write(word,num) 
			}
		}
	}

	class SumReducer extends Reducer[Text, FloatWritable, Text, FloatWritable]	{
		val mm: Array[ArrayBuffer[(Float,String)]] = new Array(15)
		val num = new FloatWritable()
		val word = new Text()
		override def setup(context:Reducer[Text, FloatWritable, Text, FloatWritable]#Context) : Unit ={
			for(i <- 1 to 12)
			{
				mm(i) = new ArrayBuffer()
			}
		}
		override def reduce(key: Text, values: Iterable[FloatWritable], context: Reducer[Text, FloatWritable, Text, FloatWritable]#Context): Unit = {
			
			var line=key.toString
			var month=(line.split(","))(0).toInt
			val t = values.asScala.toList
			val f=new FloatWritable(0)
			for( x <- t)
			{
				if(f.get<x.get)
					f.set(x.get)
			}
			val tmp=(f.get,key.toString)
			mm(month)+=tmp
			val s = mm(month).size
			if(s>5)
			{
				val x=mm(month).sorted
				mm(month).clear
				mm(month)++=x
				mm(month).trimStart(1)
			}
		}
		override def cleanup(context:Reducer[Text, FloatWritable, Text, FloatWritable]#Context) : Unit ={
			
			for(i <- 1 to 12)
			{
				val lst = mm(i).toList
				for((x,y)<-lst){
					word.set(y)
					num.set(x)
					context.write(word,num)
				}
			}
		}
	}

	def main(args: Array[String]): Unit = {

	    val configuration = new Configuration
	    val job = Job.getInstance(configuration,"Top5")
	    job.setJarByClass(this.getClass)
	    job.setMapperClass(classOf[TokenizerMapper])
	    job.setCombinerClass(classOf[SumReducer])
	    job.setReducerClass(classOf[SumReducer])
	    job.setOutputKeyClass(classOf[Text])
	    job.setOutputKeyClass(classOf[Text]);
	    job.setOutputValueClass(classOf[FloatWritable]);
	    FileInputFormat.addInputPath(job, new Path(args(0)))
	    FileOutputFormat.setOutputPath(job, new Path(args(1)))
	    System.exit(if(job.waitForCompletion(true))  0 else 1)
  	}


}