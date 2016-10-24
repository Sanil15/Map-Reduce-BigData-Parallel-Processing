package com.mapreduce.parser;


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

// Bz2WikiParser is configured as a MapReduce Job, it reads the file input of bz2 compressed type
// checks for inconsistencies, removes them. It then parses just page name and links from the file 
// and emits page name as key and its other attributes as page rank value and adjacency list. 
// Initial page rank value from Bz2Parser is set to -1.0 (garbage value) since Bz2Parsing does not do 
// any kind of page rank calculation or initialization
public class Bz2WikiParserMR {

	private static Pattern namePattern;
	private static Pattern linkPattern;

	// Maintain a global counter for counting page names 
	public static enum UpdateCounter{
		PAGENAMECOUNT;
	}

	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~|\\?]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
		// Keep only html pages not containing tilde (?????).
		//namePattern2 = Pattern.compile("^([^?????]+)$");
	}
	

	// Configured Bz2Parsing Job uses a custom mapper and an Identity Reducer 
	public static Job runMRBz2Parsing(String[] args, Configuration conf) throws Exception {
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Bz2 Parser");
		job.setJarByClass(Bz2WikiParserMR.class);
		job.setMapperClass(Bz2Mapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));
		job.waitForCompletion(true);

		return job;

	}
	

	public static class Bz2Mapper extends Mapper<Object, Text, Text, Text>{

		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try{
				Boolean doEmit = true;	
				// Configure parser.
				SAXParserFactory spf = SAXParserFactory.newInstance();
				spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
				SAXParser saxParser = spf.newSAXParser();
				XMLReader xmlReader = saxParser.getXMLReader();
				// Parser fills this list with linked page names.
				List<String> linkPageNames = new LinkedList<String>();
				xmlReader.setContentHandler(new WikiParser(linkPageNames));

				String line =  value.toString();
				// Each line formatted as (Wiki-page-name:Wiki-page-html).
				int delimLoc = line.indexOf(':');
				String pageName = line.substring(0, delimLoc);
				String html = line.substring(delimLoc + 1);
				//html = html.replace("&", "&amp;");
				Matcher matcher = namePattern.matcher(pageName);
				//Matcher matcher2 = namePattern2.matcher(pageName);

				if (!matcher.find()) {
					// Skip this html file, name contains (~).
					doEmit = false;
				}

				// Parse page and fill list of linked pages.
				linkPageNames.clear();
				try {
					xmlReader.parse(new InputSource(new StringReader(html)));
				} catch (Exception e) {
					// Discard ill-formatted pages.
					linkPageNames.clear();
					doEmit = false;
				}

				if(doEmit){

					pageName = pageName.trim();
					StringBuilder sb = new StringBuilder();

					if(linkPageNames.size() == 0)
						sb = new StringBuilder("-1.0"+"\t"+"[]");

					else{
						sb.append("-1.0");
						sb.append("\t");
						sb.append("[");
						for(String outlink : linkPageNames){
							outlink = outlink.trim();
							if(outlink.length() > 0)
							sb.append(outlink + ", ");
						}
						sb.setLength(sb.length() - 2);
						sb.append("]");
					}
					
					context.getCounter(UpdateCounter.PAGENAMECOUNT).increment((1));
					context.write(new Text(pageName), new Text(sb.toString()));
				
				}


			}catch(Exception e){
				e.printStackTrace();
			}

		}
	}


	/** Parses a Wikipage, finding links inside bodyContent div element. */
	private static class WikiParser extends DefaultHandler {
		/** List of linked pages; filled by parser. */
		private List<String> linkPageNames;
		/** Nesting depth inside bodyContent div element. */
		private int count = 0;

		public WikiParser(List<String> linkPageNames) {
			super();
			this.linkPageNames = linkPageNames;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			super.startElement(uri, localName, qName, attributes);
			if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
				// Beginning of bodyContent div element.
				count = 1;
			} else if (count > 0 && "a".equalsIgnoreCase(qName)) {
				// Anchor tag inside bodyContent div element.
				count++;
				String link = attributes.getValue("href");
				if (link == null) {
					return;
				}
				try {
					// Decode escaped characters in URL.
					link = URLDecoder.decode(link, "UTF-8");
				} catch (Exception e) {
					// Wiki-weirdness; use link as is.
				}
				// Keep only html filenames ending relative paths and not containing tilde (~).
				Matcher matcher = linkPattern.matcher(link);
				if (matcher.find()) {
					linkPageNames.add(matcher.group(1));
				}
			} else if (count > 0) {
				// Other element inside bodyContent div.
				count++;
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			super.endElement(uri, localName, qName);
			if (count > 0) {
				// End of element inside bodyContent div.
				count--;
			}
		}
	}

}
