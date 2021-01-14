package hamy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class ExtractTACKData {
	private static final DateTimeFormatter FILE_NAME_SUFFIX_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
	
	public static void main(String[] args) {
		String folderPath = null;
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			System.out.println("Enter the folder path containing TACKs: ");
			folderPath = in.readLine();
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		
		File folder = new File(folderPath);
		File[] filesInFolder = folder.listFiles();
		if(filesInFolder == null) filesInFolder = new File[0];
		System.out.println("Total number of entries in folder: " + filesInFolder.length);

		File extractFile = new File("extract_" + FILE_NAME_SUFFIX_FORMAT.format(LocalDateTime.now()) + ".txt");
		BufferedWriter extract = null;
		try {
			extractFile.createNewFile();
			extract = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(extractFile)));
		} catch (IOException e) {
			System.out.println("Unable to create extract file, " + e.getMessage());
			System.exit(-1);
		}
		
		int numFilesInFolder = 0;
		for(File file : filesInFolder) {
			if(!file.isDirectory()) {
				++numFilesInFolder;
				System.out.println(numFilesInFolder + ":> Processing file " + file.getName());
				Document doc = null;
				try {
					DocumentBuilder db = dbf.newDocumentBuilder();
					doc = db.parse(file);
				} catch(ParserConfigurationException | IOException | SAXException e) {
					System.out.println("Unable to parse file, " + e.getMessage());
					continue;
				}
				
				try {
					XPathFactory XPATH_FACTORY = XPathFactory.newInstance();
					XPath xp = XPATH_FACTORY.newXPath();
					
					String messageDate = (String) xp.evaluate("aseXML/Header/MessageDate", doc, XPathConstants.STRING);
					String messageId = (String) xp.evaluate("aseXML/Header/MessageID", doc, XPathConstants.STRING);
					
					NodeList tacks = (NodeList) xp.evaluate("aseXML/Acknowledgements/TransactionAcknowledgement", doc, XPathConstants.NODESET);
					for(int i = 0; i < tacks.getLength(); ++i) {
						Node tack = tacks.item(i);
						String initiatingTransactionID = (String) xp.evaluate("@initiatingTransactionID", tack, XPathConstants.STRING);
						String status = (String) xp.evaluate("@status", tack, XPathConstants.STRING);
						
						extract.write(initiatingTransactionID + "," + messageId + "," + status + "," + messageDate);
						extract.newLine();
					}
				} catch (XPathExpressionException e) {
					System.out.println("Unable to retrieve fields from" + e.getMessage());
					continue;
				} catch (IOException e) {
					System.out.println("Unable to write to extract file");
					continue;
				}
			}
		}
		
		System.out.println(numFilesInFolder + " processed");
		try {
			extract.flush();
			extract.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		

		
	}
}
