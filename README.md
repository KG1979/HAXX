# HAXX
package com.amex.warehouse.etl;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.NoServerForRegionException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.aexp.warehouse.exception.TableLockDetectedException;
import com.amex.warehouse.configuration.ETLConfiguration;
import com.amex.warehouse.helper.BulkLoad;
import com.amex.warehouse.helper.DBAccess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

// TODO: Auto-generated Javadoc

/**
 * 
 * The Class AAMLogLoadMR.
 */

public class ETLAAMDriver extends Configured implements Tool {

	/**
	 * 
	 * The Enum MyCounter.
	 */

	public enum MyCounter {

		/** Number of Input records. */

		InputRecords,

		/** Number of Invalid records. */

		InvalidRecords,

		/** Number of Records inserted. */

		RecordsInserted,

		/** Number of records with Invalid key format. */

		InvalidKeyFormatRecords

	}

	private static FileOutputStream pidFileOutput = null;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */

	/*
	 * This method is called from Scheduler's schedule method.It initializes
	 * configuration based on arguments, that we have passed from Scheduler's
	 * schedule method.It also checks whether Local Directories Exists or not,
	 * If not then it creates new Directories.After initializing Directories it
	 * will run job.If job is completed successfully, then it will create
	 * BulkLoadStatusDir and will create ${BulkLoadStatusDir}/status.hfiles into
	 * thatDirectory.After creating ${BulkLoadStatusDir}/status.hfiles it will
	 * call BulkLoad.load and load the table.after loading it will rename
	 * ${BulkLoadStatusDir}/status.hfiles to ${BulkLoadStatusDir}/status.load
	 */
	public int run(String[] args) {

		try {

			/* Changes starts here */

			String queueName = args[3];// Passed as argument

			/* Changes ends here */

			System.out.println("Inside " + this.getClass().getName());

			ETLConfiguration etlConfig = ETLConfiguration.getInstance();

			Configuration conf = getConf();

			long milliSeconds = 1000 * 60 * 60;

			conf.setLong("mapred.task.timeout", milliSeconds);

			conf.set("mapreduce.child.java.opts", "-Xmx1g");
			
			/*Update starts here */

			conf.set("mapred.job.queue.name", queueName);//Updated Queue name
			
			/*Update ends here*/
			
			// Adding ETL properties to configuration

			int FeedID = etlConfig.getFeedID();

			conf.set("Seperator", etlConfig.getSeperator());

			conf.set("Headers", etlConfig.getHeaders());

			conf.set("ColumnMappings", etlConfig.getColumns());

			conf.set("isKeyArray", etlConfig.getIsKeyArray());

			conf.set("rowkeyFormat", etlConfig.getRowkeyFormat());
			
			/*Update starts here */
			
			/*Getting and setting job running reverse time stamp */
			
			long reverseTimestamp = Long.MAX_VALUE - System.currentTimeMillis();
			
			conf.set("reverseTimeStamp", String.valueOf(reverseTimestamp));
			
			/*Update ends here*/

			// Printing job configuration

			System.out.println("*******************************************************************************");

			System.out.println("\t\t\tJob Configuration");

			System.out.println("-------------------------------------------------------------------------------");

			System.out.println("Seperator      :" + etlConfig.getSeperator());

			System.out.println("Headers        :" + etlConfig.getHeaders());

			System.out.println("ColumnMappings :" + etlConfig.getColumns());

			System.out.println("isKeyArray     :" + etlConfig.getIsKeyArray());

			System.out.println("rowkeyFormat   :" + etlConfig.getRowkeyFormat());

			System.out.println("HBase Table    :" + etlConfig.getHTbale());

			System.out.println("Feed ID        :" + FeedID);

			System.out.println("");

			System.out.println("Input Path     :" + args[1]);

			System.out.println("Output Path    :" + args[2]);

			System.out.println("*******************************************************************************");

			// Setting up directories

			FileSystem fs = FileSystem.get(conf);

			if (!fs.exists(new Path(etlConfig.getTableLockFileDir()))) {

				fs.mkdirs(new Path(etlConfig.getTableLockFileDir()));

			}

			// Creating a lock file at table level

			String pidLong = ManagementFactory.getRuntimeMXBean().getName();

			String[] items = pidLong.split("@");

			String pid = items[0];

			String tableLockFilePath = etlConfig.getTableLockFileDir() + "/"
					+ etlConfig.getTableID() + "_" + etlConfig.getRunFeedID()
					+ ".txt";

			File file = new File(tableLockFilePath);

			System.out.println(tableLockFilePath + ":" + file.getPath());

			FileChannel channel = new RandomAccessFile(file, "rw").getChannel();

			FileLock lock = null;

			lock = channel.tryLock(0, 0, false);

			if (lock != null)

			{

				pidFileOutput = new FileOutputStream(file);

				pidFileOutput.write(pid.getBytes());

				pidFileOutput
						.write("\n*******************************************************************************"
								.getBytes());

				pidFileOutput.write("\n\t\t\tJob Configuration".getBytes());

				pidFileOutput
						.write("\n-------------------------------------------------------------------------------"
								.getBytes());

				pidFileOutput.write(("\nSeperator      :" + etlConfig
						.getSeperator()).getBytes());

				pidFileOutput.write(("\nHeaders        :" + etlConfig
						.getHeaders()).getBytes());

				pidFileOutput.write(("\nColumnMappings :" + etlConfig
						.getColumns()).getBytes());

				pidFileOutput.write(("\nisKeyArray     :" + etlConfig
						.getIsKeyArray()).getBytes());

				pidFileOutput.write(("\nrowkeyFormat   :" + etlConfig
						.getRowkeyFormat()).getBytes());

				pidFileOutput.write(("\nHBase Table    :" + etlConfig
						.getHTbale()).getBytes());

				pidFileOutput.write(("\nFeed ID        :" + FeedID).getBytes());

				pidFileOutput.write(("\n").getBytes());

				pidFileOutput
						.write(("\nInput Path     :" + args[1]).getBytes());

				pidFileOutput
						.write(("\nOutput Path    :" + args[2]).getBytes());

				pidFileOutput
						.write(("\n*******************************************************************************")
								.getBytes());

				pidFileOutput.flush();

				System.out.println("Lock is created on this process.");

			}

			else

			{

				BufferedReader reader = new BufferedReader(new FileReader(file));

				String previousProcessID = reader.readLine();
				
				/*Changes starts here*/

				reader.close();//Closed Reader..

				/*Changes ends here*/

				throw new TableLockDetectedException(previousProcessID);

			}

			Job job = new Job(conf);

			job.setJarByClass(ETLAAMDriver.class);

			job.setJobName(args[0]);

			job.setInputFormatClass(TextInputFormat.class);

			job.setMapOutputKeyClass(ImmutableBytesWritable.class);

			job.setMapperClass(ETLMapper.class);
			
			FileInputFormat.addInputPaths(job, args[1]);

			FileSystem.getLocal(getConf()).delete(new Path(args[2]), true);

			FileOutputFormat.setOutputPath(job, new Path(args[2]));

			job.setMapOutputValueClass(Put.class);

			job.setReducerClass(PutSortReducer.class);

			job.setWorkingDirectory(new Path(args[2] + "/../tmp"));

			HFileOutputFormat.configureIncrementalLoad(job, new HTable(conf,
					etlConfig.getHTbale()));

			SimpleDateFormat dateFormat = new SimpleDateFormat(
					"yyyy-MM-dd hh:mm:ss");

			String startTime = dateFormat.format(new Date());

			job.waitForCompletion(true);

			Counters counter = job.getCounters();

			long inputRecords = counter.findCounter(
					ETLAAMDriver.MyCounter.InputRecords).getValue();

			long invalidRecords = counter.findCounter(
					ETLAAMDriver.MyCounter.InvalidRecords).getValue();

			long recordsInserted = counter.findCounter(
					ETLAAMDriver.MyCounter.RecordsInserted).getValue();

			if (job.isSuccessful()) {

				// fs.delete(new Path(file.getPath()), true);
				
				//Creating new Directories
				fs.mkdirs(new Path(etlConfig.getBulkLoadStatusDir()));

				//Creating new Files into those Directories
				fs.createNewFile(new Path(etlConfig.getBulkLoadStatusDir()
						+ "/status.hfiles"));

				// TODO

				// Need to update the database change for insertion

				// DBAccess dba = new DBAccess();

				// int recordId =
				// dba.insertInfoIntoDB("AAM Logs Load","DIGITAL","INBOUND",new
				// SimpleDateFormat("yyyy-MM-dd").format(new
				// Date()),inputRecords+"",recordsInserted+"",invalidRecords+"","1",startTime,dateFormat.format(new
				// Date()),"HFile Generated","INBOUND",1,"");
				
				//args[2] = Output Path
				BulkLoad.load(args[2], etlConfig.getHTbale());

				// dba.updateBulkLoadStatus(recordId);
				
				//Renaming Directories. ${BulkLoadStatusDir}/status.hfiles to ${BulkLoadStatusDir}/status.load 
				fs.rename(new Path(etlConfig.getBulkLoadStatusDir()
						+ "/status.hfiles"),
						new Path(etlConfig.getBulkLoadStatusDir()
								+ "/status.load"));

				return 0;

			} else {

				return -1;

			}

		} catch (NoServerForRegionException e) {

			e.printStackTrace();

			return 1;

		} catch (SQLException e) {

			e.printStackTrace();

			return 1;

		} catch (IOException e) {

			e.printStackTrace();

			return 1;

		} catch (ClassNotFoundException e) {

			e.printStackTrace();

			return 1;

		} catch (TableLockDetectedException e) {

			e.printStackTrace();

			return 40;

		} catch (Exception e) {

			e.printStackTrace();

			return 1;

		}

	}

}
------------------------
package com.amex.warehouse.etl;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.aexp.warehouse.exception.InvalidPDRecordException;
import com.amex.warehouse.configuration.PDUploadConfigSingleton;
import com.amex.warehouse.etl.ETLAAMDriver.MyCounter;

// TODO: Auto-generated Javadoc

/**
 * 
 * The Class AAMLogMapper.
 */

public class ETLMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

	/** The seperator. */

	private String seperator;

	/** The header length. */

	private int headerLength;

	/** The headers. */

	private String[] headers;

	/** The column mapping. */

	private String[] columnMapping;

	/** The rowkey format. */

	private String rowkeyFormat;

	/** The key indexes. */

	private ArrayList<String> keyIndexes;

	/** The column indexes. */

	private ArrayList<String> columnIndexes;

	/** The header value map. */

	private Map<String, String> headerValueMap;

	/** The header column map. */

	private Map<String, String> headerColumnMap;

	/** The my context. */

	private static Context myContext;

	private static String reverseTimestamp;
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
	 * Mapper.Context)
	 */

	public void setup(Context context) {

		myContext = context;

		keyIndexes = new ArrayList();

		columnIndexes = new ArrayList();

		headerValueMap = new HashMap<String, String>();

		headerColumnMap = new HashMap<String, String>();

		Configuration conf = context.getConfiguration();

		seperator = conf.get("Seperator");

		headers = conf.get("Headers").split(",");

		columnMapping = conf.get("ColumnMappings").split(",");

		rowkeyFormat = conf.get("rowkeyFormat");
		
		reverseTimestamp = conf.get("reverseTimeStamp");

		for (int i = 0; i < headers.length; i++) {

			headerColumnMap.put(headers[i], columnMapping[i]);

		}

		int i = 0;

		for (String isKey : conf.get("isKeyArray").split(",")) {

			if (isKey.equalsIgnoreCase("Y")) {

				keyIndexes.add(headers[i]);

			} else {

				columnIndexes.add(headers[i]);

			}

			i++;

		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 */

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		context.getCounter(MyCounter.InputRecords).increment(1);

		String[] values = value.toString().split(seperator, -1);

		if (values.length != headers.length) {

			context.getCounter(MyCounter.InvalidRecords).increment(1);

			context.setStatus("Values Length : " + values.length
					+ " Headers Length : " + headers.length);

		} else {

			for (int i = 0; i < headers.length; i++) {

				headerValueMap.put(headers[i], values[i]);

			}

			// String cookieRow = "AAM-" +values[1]+"-"+getValueDate(values[0]);

			String cookieRow = null;

			try {

				cookieRow = getRowKey(rowkeyFormat, keyIndexes, headerValueMap);

			} catch (Exception e) {

				context.getCounter(
						ETLAAMDriver.MyCounter.InvalidKeyFormatRecords)
						.increment(1);

			}

			if (cookieRow != null) {

				ImmutableBytesWritable recordKey = new ImmutableBytesWritable();

				recordKey.set(Bytes.toBytes(cookieRow));

				Put put = new Put(Bytes.toBytes(cookieRow));

				for (String column : columnIndexes) {

					String[] familyColumn = headerColumnMap.get(column).split("~");

					String columnFamily = familyColumn[0];

					String columnName = familyColumn[1];

					String columnValue = headerValueMap.get(column);
					
					/*Changes starts here*/
					
					columnValue =  parseColumnValue(columnValue);

					/*Changes ends here*/

					context.setStatus(cookieRow + "\t" + columnFamily + ":"
							+ columnName + "=>" + columnValue);

					put.add(columnFamily.getBytes(), columnName.getBytes(),
							columnValue.getBytes());

				}

				context.getCounter(ETLAAMDriver.MyCounter.RecordsInserted).increment(1);

				context.write(recordKey, put);

			}

		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce
	 * .Mapper.Context)
	 */

	public void cleanup(Context context) {

		Configuration conf = context.getConfiguration();

		conf.set("Completion", "Success");

	}

	/**
	 * 
	 * Gets the value date.
	 * 
	 * 
	 * 
	 * @param recordDateTimeStamp
	 *            the record date time stamp
	 * 
	 * @return the value date
	 */

	public static String getValueDate(String recordDateTimeStamp) {

		SimpleDateFormat outputFormat = new SimpleDateFormat(
				PDUploadConfigSingleton.getInstance().getValueDateFormat());

		final List<String> dateFormats = Arrays.asList("yyyy-MM-dd HH:mm:ss",
				"yyyy-MM-dd,HH:mm:ss");

		for (String format : dateFormats) {

			SimpleDateFormat sdf = new SimpleDateFormat(format);

			try {

				if (sdf.parse(recordDateTimeStamp) != null) {

					return outputFormat.format(sdf.parse(recordDateTimeStamp));

				}

			} catch (ParseException e) {

				// intentionally empty

			}

		}

		throw new IllegalArgumentException("Invalid input for date. Given '" +

		recordDateTimeStamp +

		"', expecting format yyyy-MM-dd HH:mm:ss or yyyy-MM-dd,HH:mm:ss");

	}

	/**
	 * 
	 * Gets the format.
	 * 
	 * 
	 * 
	 * @param rowkeyFormat
	 *            the rowkey format
	 * 
	 * @param header
	 *            the header
	 * 
	 * @return the format
	 */

	private static String getFormat(String rowkeyFormat, String header) {

		return rowkeyFormat.substring(rowkeyFormat.indexOf(header + "[")
				+ (header + "[").length(),
				rowkeyFormat.indexOf("]", rowkeyFormat.indexOf(header)));

	}

	/**
	 * 
	 * Gets the value replaced with format.
	 * 
	 * 
	 * 
	 * @param rowkeyFormat
	 *            the rowkey format
	 * 
	 * @param header
	 *            the header
	 * 
	 * @param headerValueMap
	 *            the header value map
	 * 
	 * @return the value replaced with format
	 * 
	 * @throws Exception
	 *             the exception
	 */

	private static String getValueReplacedWithFormat(String rowkeyFormat,
			String header, Map<String, String> headerValueMap) throws Exception {

		String format = getFormat(rowkeyFormat, header);

		String[] formatAttributes = format.split("~");

		String rowKeyValueToBeReplaced = "";

		if (formatAttributes.length == 3) {

			if (formatAttributes[0].equalsIgnoreCase("DATE")) {

				Date inputDate;

				if (formatAttributes[1].equalsIgnoreCase("SYSDATE")) {

					inputDate = new Date();

				} else {

					inputDate = new SimpleDateFormat(formatAttributes[1])
							.parse(headerValueMap.get(header));

				}

				rowKeyValueToBeReplaced = new SimpleDateFormat(
						formatAttributes[2]).format(inputDate);

			}

		} else {

			throw new Exception(); // RowKey format Exception

		}

		return rowkeyFormat.replace("<" + header + "[" + format + "]>",
				rowKeyValueToBeReplaced);

	}

	/**
	 * 
	 * Gets the row key.
	 * 
	 * 
	 * 
	 * @param rowkeyFormat
	 *            the rowkey format
	 * 
	 * @param headersInRowKey
	 *            the headers in row key
	 * 
	 * @param headerValueMap
	 *            the header value map
	 * 
	 * @return the row key
	 * 
	 * @throws Exception
	 *             the exception
	 */

	private static String getRowKey(String rowkeyFormat,
			List<String> headersInRowKey, Map<String, String> headerValueMap)
			throws Exception {
		
		String inpuRowKeyFormat = rowkeyFormat;

		ArrayList<String> variablesList = new ArrayList<String>();

		ArrayList<String> variablesInKey = getVarsinRowKey(inpuRowKeyFormat);

		for (String header : headersInRowKey) 
		{

			if (inpuRowKeyFormat.contains(header)) 
			{

				if (inpuRowKeyFormat.contains(header + "[")
						&& headerValueMap.containsKey(header)) 
				{

					inpuRowKeyFormat = getValueReplacedWithFormat(
							inpuRowKeyFormat, header, headerValueMap);

				} else
				{

					inpuRowKeyFormat = inpuRowKeyFormat.replace("<" + header
							+ ">", headerValueMap.get(header));

				}

			}
			else 
			{

				if (getFormat(rowkeyFormat, header).split("~")[0]
						.equalsIgnoreCase("DATE")) 
				{

					FileSplit fileSplit = (FileSplit) myContext.getInputSplit();

					FileSystem fs = FileSystem
							.get(myContext.getConfiguration());

					FileStatus status = fs.getFileStatus(fileSplit.getPath());

					String timeStamp = new SimpleDateFormat("yyMMdd-HHmmss")
							.format(new Date(status.getModificationTime()));

					inpuRowKeyFormat = rowkeyFormat
							.replace(
									"<" + header + "["
											+ getFormat(rowkeyFormat, header)
											+ "]>", timeStamp);
					

					// inpuRowKeyFormat =
					// rowkeyFormat.replace("<"+header+"["+getFormat(rowkeyFormat,
					// header)+"]>", reverseTimestamp+"");

				}
				
				/*Changes starts here*/
				else if (getFormat(rowkeyFormat, header).split("~")[0]
						.equalsIgnoreCase("RDATE")) {

					FileSplit fileSplit = (FileSplit) myContext.getInputSplit();

					FileSystem fs = FileSystem
							.get(myContext.getConfiguration());

					FileStatus status = fs.getFileStatus(fileSplit.getPath());

					String timeStamp = new SimpleDateFormat("yyMMdd-HHmmss")
							.format(new Date(status.getModificationTime()));

					// inpuRowKeyFormat =
					// rowkeyFormat.replace("<"+header+"["+getFormat(rowkeyFormat,
					// header)+"]>", timeStamp);;
					inpuRowKeyFormat = rowkeyFormat.replace("<" + header + "["
							+ getFormat(rowkeyFormat, header) + "]>",
							reverseTimestamp + "");
					

				}
				/*Changes ends here*/
				else
				{

					throw new Exception(); // RowKey format Exception

				}

			}

			variablesInKey.remove(header);

		}

		for (String header : variablesInKey) 
		{

			if (inpuRowKeyFormat.contains(header))
			{

				if (inpuRowKeyFormat.contains(header + "[")) 
				{

					if (getFormat(rowkeyFormat, header).split("~")[0]
							.equalsIgnoreCase("DATE")) 
					{

						FileSplit fileSplit = (FileSplit) myContext.getInputSplit();

						FileSystem fs = FileSystem.get(myContext.getConfiguration());

						FileStatus status = fs.getFileStatus(fileSplit.getPath());

						String timeStamp = new SimpleDateFormat("yyMMdd-HHmmss")
								.format(new Date(status.getModificationTime()));

						inpuRowKeyFormat = inpuRowKeyFormat.replace(
								"<" + header + "["
										+ getFormat(inpuRowKeyFormat, header)
										+ "]>", timeStamp);
						
						// inpuRowKeyFormat =
						// inpuRowKeyFormat.replace("<"+header+"["+getFormat(inpuRowKeyFormat,
						// header)+"]>", reverseTimestamp+"");

					} 
					/*Changes starts here*/
					else if (getFormat(rowkeyFormat, header).split("~")[0]
							.equalsIgnoreCase("RDATE")) {

						FileSplit fileSplit = (FileSplit) myContext
								.getInputSplit();

						FileSystem fs = FileSystem.get(myContext
								.getConfiguration());

						FileStatus status = fs.getFileStatus(fileSplit
								.getPath());

						String timeStamp = new SimpleDateFormat("yyMMdd-HHmmss")
								.format(new Date(status.getModificationTime()));

						// inpuRowKeyFormat =
						// inpuRowKeyFormat.replace("<"+header+"["+getFormat(inpuRowKeyFormat,
						// header)+"]>", timeStamp);
						inpuRowKeyFormat = inpuRowKeyFormat.replace(
								"<" + header + "["
										+ getFormat(inpuRowKeyFormat, header)
										+ "]>", reverseTimestamp + "");

					}

					/*Changes ends here*/
					else
					{

						throw new Exception(); // RowKey format Exception

					}

				}
				else 
				{

					throw new Exception(); // RowKey format Exception

				}

			}

		}

		return inpuRowKeyFormat;

	}

	/**
	 * 
	 * Gets the varsin row key.
	 * 
	 * 
	 * 
	 * @param rowkeyFormat
	 *            the rowkey format
	 * 
	 * @return the varsin row key
	 */

	private static ArrayList<String> getVarsinRowKey(String rowkeyFormat) {

		String inpuRowKeyFormat = rowkeyFormat;

		ArrayList<String> variablesList = new ArrayList<String>();

		if (rowkeyFormat.contains("<") && rowkeyFormat.contains(">")) {

			while (inpuRowKeyFormat.contains("<")
					|| inpuRowKeyFormat.contains(">")) {

				int beginIndex = inpuRowKeyFormat.indexOf("<");

				int endIndex = inpuRowKeyFormat.indexOf(">");

				String variableExtract = inpuRowKeyFormat.substring(
						beginIndex + 1, endIndex);

				if (variableExtract.contains("[")) {

					variableExtract = variableExtract.substring(0,
							variableExtract.indexOf("["));

				} else {

					System.out
							.println("No format specied, variable will be use as is.");

				}

				variablesList.add(variableExtract);

				inpuRowKeyFormat = inpuRowKeyFormat.substring(endIndex + 1);

			}

		} else {

			System.out.println("Rowkey doesnt have variables");

		}

		return variablesList;

	}
	
	/*Changes starts here*/
	
	/*
	 * This method removes all single quotes and double quotes from 
	 * string value.
	 * */
	private static String parseColumnValue(String value)
	{
		
		try
		{
			
			if(value.contains("'"))
			{

				value = value.replace('\'', ' ');
				
			}
			if(value.contains("\""))
			{
				
				value = value.replace('\"', ' ');
				
			}
			
		}
		catch(Exception exception)
		{
			
			exception.printStackTrace();
			
		}
		
		return value;
		
	}
	
	/*Changes ends here*/
	

}
------------------------------
