package hamy;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class FindXML {
	private final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	private final XPathFactory xpf = XPathFactory.newInstance();
	
	public static void main(String[] args) {
		if(args.length < 2) {
			System.out.println("Usage: FindXML \"Folder Path\" \"Filter XPath Expression\" \"Value XPath Expression\" ");
			System.exit(-1);
		}
		
		String folderPath = args[0];
		String valueXPath = args[1];
		
		String filterXPath = null;
		if(args.length > 2)
			filterXPath = args[2];
		
		FindXML app = new FindXML();
		File folder = new File(folderPath);
		app.start(folder, filterXPath, valueXPath);
	}
	
	private void start(File folder, String valueXPath, String filterXPath) {
		XPathExpression valuexpe = null, filterxpe = null;
		try {
			XPath xp = xpf.newXPath();
			if(valueXPath != null)
				valuexpe = xp.compile(valueXPath);
			
			if(filterXPath != null)
				filterxpe = xp.compile(filterXPath);
			
		} catch (XPathExpressionException e) {
			e.printStackTrace();
			throw new RuntimeException("Unable to compile XPath expression", e);
		}
		
		for(File file : folder.listFiles()) {
			evaluate(file, filterxpe, valuexpe);
		}
		
	}
	
	private void evaluate(File file, XPathExpression filterxpExpression, XPathExpression valuexpExpression) {
		if(file.isDirectory())
			return;
		
		DocumentBuilder db;
		try {
			db = dbf.newDocumentBuilder();
			Document doc = db.parse(file);
			
			if(filterxpExpression == null || (Boolean) filterxpExpression.evaluate(doc, XPathConstants.BOOLEAN)) {
				if(valuexpExpression != null)
					System.out.println(file.getName() + ": " + ((String) valuexpExpression.evaluate(doc, XPathConstants.STRING)));
				else
					System.out.println(file.getName());
			}
		} catch (ParserConfigurationException e) {
		} catch (SAXException e) {
		} catch (IOException e) {
		} catch (XPathExpressionException e) {
		}
	}
}
