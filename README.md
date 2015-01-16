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

		if (values.length != headers.length)
		{

			context.getCounter(MyCounter.InvalidRecords).increment(1);

			context.setStatus("Values Length : " + values.length
					+ " Headers Length : " + headers.length);

		}
		else
		{

			for (int i = 0; i < headers.length; i++) 
			{

				headerValueMap.put(headers[i], values[i]);

			}

			// String cookieRow = "AAM-" +values[1]+"-"+getValueDate(values[0]);

			String cookieRow = null;

			try
			{

				cookieRow = getRowKey(rowkeyFormat, keyIndexes, headerValueMap);
				
				String [] cookieRows = cookieRow.split(".");
				
				String textToBeReplaced = padZeroesAtLeftInKeyColumnValue(cookieRows[0]);
				
				cookieRow = cookieRow.replace(cookieRows[0], textToBeReplaced);

			} 
			catch (Exception e)
			{

				context.getCounter(
						ETLAAMDriver.MyCounter.InvalidKeyFormatRecords)
						.increment(1);

			}

			if (cookieRow != null) 
			{

				ImmutableBytesWritable recordKey = new ImmutableBytesWritable();

				recordKey.set(Bytes.toBytes(cookieRow));

				Put put = new Put(Bytes.toBytes(cookieRow));

				for (String column : columnIndexes)
				{

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

	public void cleanup(Context context)
	{

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

	public static String getValueDate(String recordDateTimeStamp)
{

		SimpleDateFormat outputFormat = new SimpleDateFormat(
				PDUploadConfigSingleton.getInstance().getValueDateFormat());

		final List<String> dateFormats = Arrays.asList("yyyy-MM-dd HH:mm:ss",
				"yyyy-MM-dd,HH:mm:ss");

		for (String format : dateFormats)
		{

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

		if (formatAttributes.length == 3) 
		{

			if (formatAttributes[0].equalsIgnoreCase("DATE")) 
			{

				Date inputDate;

				if (formatAttributes[1].equalsIgnoreCase("SYSDATE")) 
				{

					inputDate = new Date();

				}
				else 
				{

					inputDate = new SimpleDateFormat(formatAttributes[1]).parse(headerValueMap.get(header));

				}

				rowKeyValueToBeReplaced = new SimpleDateFormat(formatAttributes[2]).format(inputDate);

			}

		} else
		{

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
		
		String inputRowKeyFormat = rowkeyFormat;

		ArrayList<String> variablesInKey = getVarsinRowKey(inputRowKeyFormat);

		for (String header : headersInRowKey) 
		{

			if (inputRowKeyFormat.contains(header)) 
			{

				if (inputRowKeyFormat.contains(header + "[") && headerValueMap.containsKey(header)) 
				{

					inputRowKeyFormat = getValueReplacedWithFormat(inputRowKeyFormat, header, headerValueMap);

				}
				else
				{

					String valueToBeReplaced = padZeroesAtLeftInKeyColumnValue(headerValueMap.get(header));
					
					valueToBeReplaced = valueToBeReplaced+"-"+ reverseTimestamp;
					
					inputRowKeyFormat = inputRowKeyFormat.replace("<" + header
							+ ">", valueToBeReplaced);//Updated here
					
					//inputRowKeyFormat = inputRowKeyFormat.replace("<" + header + ">", headerValueMap.get(header));
					
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
					
					String valueToBeReplaced = padZeroesAtLeftInKeyColumnValue(headerValueMap.get(header));
					
					valueToBeReplaced = valueToBeReplaced+"-"+timeStamp;

					inputRowKeyFormat = rowkeyFormat
							.replace(
									"<" + header + "["
											+ getFormat(rowkeyFormat, header)
											+ "]>", valueToBeReplaced);
					
					/*inputRowKeyFormat = rowkeyFormat
							.replace(
									"<" + header + "["
											+ getFormat(rowkeyFormat, header)
											+ "]>", timeStamp);*/
					

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
					
					String valueToBeReplaced = padZeroesAtLeftInKeyColumnValue(headerValueMap.get(header));
					
					valueToBeReplaced = valueToBeReplaced +"-"+ reverseTimestamp;
					
					inputRowKeyFormat = rowkeyFormat.replace("<" + header + "["
							+ getFormat(rowkeyFormat, header) + "]>",
							valueToBeReplaced + "");
					
					/*inputRowKeyFormat = rowkeyFormat.replace("<" + header + "["
							+ getFormat(rowkeyFormat, header) + "]>",
							reverseTimestamp + "");*/
					
					

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

			if (inputRowKeyFormat.contains(header))
			{

				if (inputRowKeyFormat.contains(header + "[")) 
				{

					if (getFormat(rowkeyFormat, header).split("~")[0]
							.equalsIgnoreCase("DATE")) 
					{

						FileSplit fileSplit = (FileSplit) myContext.getInputSplit();

						FileSystem fs = FileSystem.get(myContext.getConfiguration());

						FileStatus status = fs.getFileStatus(fileSplit.getPath());

						String timeStamp = new SimpleDateFormat("yyMMdd-HHmmss")
								.format(new Date(status.getModificationTime()));

						
						String valueToBeReplaced = padZeroesAtLeftInKeyColumnValue(headerValueMap.get(header));
						
						valueToBeReplaced = valueToBeReplaced+"-"+timeStamp;
						
						inputRowKeyFormat = inputRowKeyFormat.replace(
								"<" + header + "["
										+ getFormat(inputRowKeyFormat, header)
										+ "]>", valueToBeReplaced);
						
						/*inputRowKeyFormat = inputRowKeyFormat.replace(
								"<" + header + "["
										+ getFormat(inputRowKeyFormat, header)
										+ "]>", timeStamp);*/
						
						
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
						
						String valueToBeReplaced = padZeroesAtLeftInKeyColumnValue(headerValueMap.get(header));
						
						valueToBeReplaced = valueToBeReplaced+"-"+reverseTimestamp;
						
						inputRowKeyFormat = inputRowKeyFormat.replace(
								"<" + header + "["
										+ getFormat(inputRowKeyFormat, header)
										+ "]>", valueToBeReplaced + "");
						
						/*inputRowKeyFormat = inputRowKeyFormat.replace(
								"<" + header + "["
										+ getFormat(inputRowKeyFormat, header)
										+ "]>", reverseTimestamp + "");*/

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

		return inputRowKeyFormat;

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

		String inputRowKeyFormat = rowkeyFormat;

		ArrayList<String> variablesList = new ArrayList<String>();

		if (rowkeyFormat.contains("<") && rowkeyFormat.contains(">"))
		{

			while (inputRowKeyFormat.contains("<")
					|| inputRowKeyFormat.contains(">")) 
			{

				int beginIndex = inputRowKeyFormat.indexOf("<");

				int endIndex = inputRowKeyFormat.indexOf(">");

				String variableExtract = inputRowKeyFormat.substring(beginIndex + 1, endIndex);

				if (variableExtract.contains("["))
				{

					variableExtract = variableExtract.substring(0,variableExtract.indexOf("["));

				} 
				else 
				{

					System.out.println("No format specied, variable will be use as is.");

				}

				variablesList.add(variableExtract);

				inputRowKeyFormat = inputRowKeyFormat.substring(endIndex + 1);

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
	
	private static String padZeroesAtLeftInKeyColumnValue(String string)
	{
		
		return string.length() >= 11 ? string : "00000000000".substring(string.length()) + string;  
		
	} 
	
	/*Changes ends here*/
	
	
	

}

