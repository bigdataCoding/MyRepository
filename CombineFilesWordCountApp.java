package offline4.examples.mapreduce.combinefile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 原始数据是含有大量小文件的文件夹，使用该类，可以把大量小文件运行在一个map task中
 * @author 吴超
 *
 */
public class CombineFilesWordCountApp {
	/**
	 * 驱动代码
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		//从命令行传入输入路径
		String inputPath = args[0];
		//从命令行传入输出目录
		Path outputDir = new Path(args[1]);

		Configuration conf = new Configuration();
		outputDir.getFileSystem(conf).delete(outputDir, true);
		//表示job名称，可以自定义，一般是类名
		String jobName = CombineFilesWordCountApp.class.getSimpleName();
		//把所有的相关内容都封装到job中
		Job job = Job.getInstance(conf, jobName);
		//打成jar运行必备代码
		job.setJarByClass(CombineFilesWordCountApp.class);
		
		//
		job.setInputFormatClass(CombineSmallFilesInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		//设置输入路径
		FileInputFormat.setInputPaths(job, inputPath);
		//设置输出目录
		FileOutputFormat.setOutputPath(job, outputDir);
		
		//设置自定义mapper类
		job.setMapperClass(HelloWordCountMapper.class);
		//指定k2,v2类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		
		//设置自定义reduce类
		job.setReducerClass(HelloWordCountReducer.class);
		//指定k3，v3类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//提交给yarn运行，等待结束
		job.waitForCompletion(true);
	}
	
	
	
	/**
	 * map过程。
	 * 在这里，程序员继承Mapper，覆盖map(...)方法。
	 * 该类在运行的时候，称作map task，是一个java进程。
	 * ----------------------------------------------------
	 * map()全部执行完后，产生的<k2,v2>有4个，即<hello,1><you,1><hello,1><me,1>。
	 * 排序后是<hello,1><hello,1><me,1><you,1>。
	 * 分组后是<hello,{1,1}><me,{1}><you,{1}>。
	 * @author 吴超
	 *
	 */
	public static class HelloWordCountMapper  extends Mapper<LongWritable, Text, Text, LongWritable>{
		Text k2 = new Text();
		LongWritable v2 = new LongWritable();
		
		/**
		 * 前面已经有拆分完成的<k1,v1>。调用map()一次方法，就处理一个<k1,v1>对。
		 * 
		 * 在map()方法，拆分每一行，得到每个单词，每个单词(不是每个不同的单词)的出现次数是1。
		 * 构造<k2,v2>，k2表示单词，v2表示出现次数1。
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			//因为要对每行内容做拆分，需要调用String.split()，所以需要把Text转行成String。
			String line = value.toString();
			//拆分每行内容，结果是单词的数组
			String[] splited = line.split("\t");
			//循环数组，取每个单词。在for循环中构造<k2,v2>
			for (String word : splited) {
				k2.set(word);
				v2.set(1L);
				//把<k2,v2>写出去，相当于调用return语句
				context.write(k2, v2);
			}
		}
	}
	
	
	/**
	 * reduce过程
	 * 
	 * reduce端接收的是map的输出，即4个<k2,v2>，3个分组。
	 * 在reduce执行之前，reduce端合并、排序、分组<k2,v2>。
	 * 在reduce()调用之前，有3个分组，即<hello,{1,1}><me,{1}><you,{1}>
	 * 一次reduce()执行，处理1个分组。所以说，执行3次reduce()。
	 * ------------------------------------------------------------------
	 * reduce task执行结束后，框架会把reduce输出的<k3,v3>写入到HDFS中。
	 * @author 吴超
	 *
	 */
	public static class HelloWordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		LongWritable v3 = new LongWritable();

		/**
		 * k2表示每个不同的单词
		 * v2s表示每个不同的单词的出现次数
		 * 在reduce()中，只需要汇总v2s中的出现次数就行。
		 */
		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2s,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			//sum表示当前单词k2出现的总次数
			long sum = 0L;
			for (LongWritable v2 : v2s) {
				sum += v2.get();
			}
			//k3表示当前不同的单词，与k2含义相同
			
			v3.set(sum);
			context.write(k2, v3);
		}
	}
	
	/**
	 * 自定义InputFormat，继承CombineFileInputFormat
	 * @author 吴超
	 *
	 */
	public static class CombineSmallFilesInputFormat extends CombineFileInputFormat<LongWritable, Text>{

		//实现该方法
		@Override
		public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
				throws IOException {
			//创建一个CombineFileRecordReader对象，该对象负责遍历所有小文件
			//该对象的形参是3个，第一个需要强转为CombineInputSplit类型，第三个是自定义的RecordReader
			return new CombineFileRecordReader<LongWritable, Text>((CombineFileSplit)split, context, CombineSmallFilesRecordReader.class);
		}
		
	}
	
	/**
	 * 自定义的RecordReader是处理读取每个小文件中的内容，转换为<k1,v1>
	 * @author 吴超
	 *
	 */
	public static class CombineSmallFilesRecordReader extends RecordReader<LongWritable, Text>{
		private LineRecordReader lrr;
		/**
		 * 一定要定义有这三个形参的构造方法
		 * @param split 
		 * @param context
		 * @param index 表示当前小文件的索引
		 * @throws IOException 
		 */
		public CombineSmallFilesRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
			//因为都是按行处理，所以处理逻辑相同，反射LineRecordReader
			this.lrr = ReflectionUtils.newInstance(LineRecordReader.class, context.getConfiguration());

			//获取当前文件的信息
			Path path = split.getPath(index);
			long start = split.getOffset(index);
			long length = split.getLength(index);
			String[] locations = split.getLocations();
			//构造filesplit
			FileSplit fileSplit = new FileSplit(path, start, length, locations);
			//必须调用initialize(...)
			this.lrr.initialize(fileSplit, context);
		}
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return lrr.nextKeyValue();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return lrr.getCurrentKey();
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return lrr.getCurrentValue();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return lrr.getProgress();
		}

		@Override
		public void close() throws IOException {
			if(lrr!=null)lrr.close();
		}
		
	}
}
