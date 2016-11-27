package com.mapreduce.parser;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.mapreduce.parser.Bz2WikiParserMR.UpdateCounter;

// Bz2Mapper reads Bz2 compressed file, uses Bz2WikiParser to parse the file and emits the following: 
// (PageName, Outlink) for all outlinks in the adjacency list. This is important to recover the graph structure in reducer.
// (Outlink,"") Note. value is ignored for this case. This is important to identify total nodes in graph and its value is ignored in reducer
// (PageName, "") Note. value is ignored for this case. This is important to identify dead nodes (Nodes with outlinks but no inlinks) and its value is ignored in reducer
public class Bz2Mapper extends Mapper<Object, Text, Text, Text>{

	private static Pattern namePattern;
	private static Pattern linkPattern;
	
	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~|\\?]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");		
	}
	
	
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
			Matcher matcher = namePattern.matcher(pageName);
			
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
				
				if(linkPageNames.size() > 0){
					for(String outlink : linkPageNames){
						outlink = outlink.trim();
						// Emit PageName and Outlink
						context.write(new Text(pageName), new Text(outlink));
						// Emit Outlink and Empty Value
						context.write(new Text(outlink), new Text(""));
					}
				}else{
					// Emit PageName and Empty Value
					context.write(new Text(pageName), new Text(""));
				}
				
			}


		}catch(Exception e){
			e.printStackTrace();
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
