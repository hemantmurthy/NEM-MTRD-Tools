package hamy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.text.DecimalFormat;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

public class ConvertNEM12Intervals {
	private String sourceFolderPath = null;
	private int newIntervalSize;
	
	public static void main(String[] args) {
		// Get input for the program ...
		BufferedReader in = null;
		in = new BufferedReader(new InputStreamReader(System.in));

		String folderPath = ".";
		
		try {
			while(true) {
				System.out.println("Enter the folder path containing NEM12s to be converted: ");
				folderPath = in.readLine();
				if(folderPath == null || "".equals(folderPath.trim()))
					folderPath = ".";
				
				File folder = new File(folderPath);
				if(!folder.exists())
					System.out.println("Path specified does not exist");
				else
					break;
			}
			
		} catch (IOException e) {
			throw new RuntimeException("Unable to read console", e);
		}

		int newIntervalSize = 0;
		try {
			while(true) {
				System.out.println("Enter the size of interval to convert to (15 or 5): ");
				newIntervalSize = Integer.parseInt(in.readLine());
				if(newIntervalSize == 5 || newIntervalSize == 15)
					break;
				else
					System.out.println("Unable to convert file to specified interval size " + newIntervalSize);
			}
		} catch (IOException e) {
			throw new RuntimeException("Unable to read console", e);
		} catch(NumberFormatException e) {
			throw new RuntimeException("Invalid interval size", e);
		}
		
		// Start conversion ...
		ConvertNEM12Intervals app = new ConvertNEM12Intervals(folderPath, newIntervalSize);
		app.convert();
	}
	
	ConvertNEM12Intervals(String sourceFolderPath, int newIntervalSize) {
		this.sourceFolderPath = sourceFolderPath;
		this.newIntervalSize = newIntervalSize;
	}
	
	void convert() {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    	TransformerFactory transformerFactory = TransformerFactory.newInstance();
    	XPathFactory xpf = XPathFactory.newInstance();
    	
    	// Check source folder ...
		File sourceFolder = new File(sourceFolderPath);
		if(!sourceFolder.exists()) {
			System.out.println("Folder " + sourceFolder.getAbsolutePath() + " does not exist");
			return;
		}
		
		File[] filesInFolder = null;
		if(sourceFolder.isDirectory())
			filesInFolder = sourceFolder.listFiles();
		else {
			System.out.println("Source path " + sourceFolder.getAbsolutePath() + " is not a folder");
			return;
		}
		
		// Setup destination folder where converted files will be written to ...
		File destFolder = new File(sourceFolder, "converted");
		if(destFolder.exists()) {
			if(!destFolder.isDirectory()) {
				System.out.println("Destination path " + destFolder.getAbsolutePath() + " is not a folder");
				return;
			}
			System.out.println("Destination folder named " + destFolder.getAbsolutePath() + " already exists. Converted files will be writted to this folder");
		} else {
			System.out.println("Creating destination folder named " + destFolder.getName() + " in source folder " + sourceFolder.getAbsolutePath());
			destFolder.mkdir();
		}
		
   	
		// Loop through each file in the folder ...
		File destFile = null;
		int filesProcessed = 0, lastPerc = 0;
	
		for(File f : filesInFolder) {
			++filesProcessed;
			if(f.isFile()) {
				// Try to parse it as an XML file ...
				Document doc = null;
				try {
					DocumentBuilder db = dbf.newDocumentBuilder();
					doc = db.parse(f);
				} catch(ParserConfigurationException | IOException | SAXException e) {
					// If it cannot be parsed as XML, skip this file ...
					System.out.println("Unable to parse file " + f.getName() + ", " + e.getMessage());
					continue;
				}
				
				// Check if this is a NEM12 file and contains interval data nodes ...
				XPath xp = xpf.newXPath();
				NodeList intervalDataNodes = null;
				try {
					if((Boolean) xp.evaluate("string(aseXML/Header/TransactionGroup) = 'MTRD' and boolean(aseXML/Transactions/Transaction/MeterDataNotification/CSVIntervalData)", doc, XPathConstants.BOOLEAN))
						intervalDataNodes = (NodeList) xp.evaluate("aseXML/Transactions/Transaction/MeterDataNotification/CSVIntervalData", doc, XPathConstants.NODESET);
					else {
						System.out.println("Skipping file " + f.getName() + " as it does not seem to be a NEM12 file, and contains no interval data");
						continue;
					}
				} catch (XPathExpressionException e) {
					System.out.println("Unable to retrieve interval data from file" + e.getMessage());
					continue;
				}
				
				if(intervalDataNodes.getLength() == 0) {
					System.out.println("Skipping file " + f.getName() + " as no interval data found");
					continue;
				}
				
				// Loop through each interval data node in the file ...
				Node dataNode = null, dataTextNode = null;
				Text dataText = null;
				for(int i = 0; i < intervalDataNodes.getLength(); ++i) {
					dataNode = intervalDataNodes.item(i);
					NodeList ns = dataNode.getChildNodes();
					if(ns.getLength() > 0) {
						// Extract the CSV data from the interval data node and substitute it with modified CSV data for new interval size ...
						dataTextNode = ns.item(0);
						if(dataTextNode instanceof Text) {
							dataText = (Text) dataTextNode;
							dataText.setData(getConvertedData(dataText.getData(), newIntervalSize));
						}
					}
				}
				
				
				// Write output XML
				destFile = new File(destFolder, f.getName());
	            try {
	            	Transformer transformer = transformerFactory.newTransformer();
	            	DOMSource domSource = new DOMSource(doc);
	            	StreamResult streamResult = new StreamResult(destFile);
	            	
	            	transformer.transform(domSource, streamResult);
				} catch (TransformerConfigurationException e) {
					System.out.println("Unable to create Transformer");
					continue;
				} catch (TransformerException e) {
					System.out.println("Unable to transform and write output file");
					continue;
				}
				
			}
			
			int perc = ((filesProcessed * 20) / filesInFolder.length) * 5;
			if(perc > lastPerc) {
				System.out.println(perc + " % files processed");
				lastPerc = perc;
			}
		}
	}
	
	private String getConvertedData(String data, int requiredIntervalSize) {
		try {
			BufferedReader in = new BufferedReader(new StringReader(data));
			StringWriter outsw = new StringWriter();
			BufferedWriter out = new BufferedWriter(outsw);
			
			String line, modifiedLine;
			boolean conversionRequired = false;
			int convFactor = 1;
			
			// Read the input CSV data line by line ...
			while((line = in.readLine()) != null) {
				// Get record type of the line ...
				switch(line.substring(0, 3)) {
				
				// Meter channel record (200). Determine if conversion is required, and if so, set the conversion factor.
				// This factor will be used to convert all read (300) records that follow, till the next meter channel record ... 
				case "200": 
					String[] val200 = line.split(",", -1);
					int intSize = Integer.parseInt(val200[8]); // Get interval size for the meter ...
					
					// Determine if conversion is required, and if so, the conversion factor ...
					if(intSize == 30) {
						if(requiredIntervalSize == 15) {
							conversionRequired = true;
							convFactor = 2;
						} else if(requiredIntervalSize == 5) {
							conversionRequired = true;
							convFactor = 6;
						} else 
							conversionRequired = false;
					} else if(intSize == 15) {
						if(requiredIntervalSize == 5) {
							conversionRequired = true;
							convFactor = 3;
						} else
							conversionRequired = false;
					}
					
					if(conversionRequired) {
						// If conversion is required, modify the 200 record by setting the new interval size .... 
						val200[8] = Integer.toString(requiredIntervalSize);
						modifiedLine = String.join(",", val200) + "\n";
					} else {
						// else leave the record unmodified ...
						modifiedLine = line + "\n";
					}
					break;
				case "300":
					if(conversionRequired) {
						// If conversion is required, split the 300 record ...
						String[] val300 = line.split(",", -1);
						
						// Create an array to hold the modified 300 record with required number of intervals ...
						String[] newVal300 = new String[(val300.length - 7) * convFactor + 7];
						
						// Copy values from current 300 record to the modified 300 record that will not change ...
						newVal300[0] = val300[0];
						newVal300[1] = val300[1];
						for(int i = 1; i < 6; ++i)
							newVal300[newVal300.length - i] = val300[val300.length - i];
						
						// Create the interval values for the modified 300 record ...
						// Example: The 30 minute record below
						// 300,20201014,6.000,12.600,3.000,1.500, .... 48 values,
						// 
						// will be converted to 5 mins as below ...
						// 300,20201016,1.000,1.000,1.000,1.000,1.000,1.000,2.100,2.100,2.100,2.100,2.100,2.100,0.500,0.500,0.500,0.500,0.500,0.500, ... 288 values
						// Use the first entry to determine the format of the interval value ...
						/*
						String vs = val300[2];
						int ad = vs.indexOf('.');
						ad = ad == -1 ? 0 : vs.length() - ad - 1;
						DecimalFormat df = ad > 0 ? new DecimalFormat("0." + "000000000000000000000".substring(0, ad)) : new DecimalFormat("0");
						*/
						
						DecimalFormat df = new DecimalFormat("0.000");
						
						// Loop through every interval value in the current 300 record ...
						double v;
						for(int i = 0; i < val300.length - 7; ++i) {
							// For each interval value, calculate the value of each new interval ...
							v = Double.parseDouble(val300[i + 2]) / convFactor;
							// Repeat this value the required number of times in the modified 300 record ...
							for(int j = 0; j < convFactor; ++j)
								newVal300[2 + i * convFactor + j] = df.format(v);
						}
						
						modifiedLine = String.join(",", newVal300) + "\n";
					} else {
						// Leave the record unchanged if conversion is not required ...
						modifiedLine = line + "\n";
					}
					break;
				case "400":
					if(conversionRequired) {
						// If conversion is required, modify the from and to interval to reflect the correct intervals 
						// on the modified 300 record ...
						String[] val400 = line.split(",", -1);
						val400[1] = Integer.toString((Integer.parseInt(val400[1]) - 1) * convFactor + 1);
						val400[2] = Integer.toString(Integer.parseInt(val400[2]) * convFactor);
						modifiedLine = String.join(",", val400) + "\n";
					} else 
						// Leave the record unchanged if conversion is not required ...
						modifiedLine = line + "\n";
					break;
				case "900":
					// No change needed for 900 record. Skip the newline though ...
					modifiedLine = line;
					break;
				default:
					// No change needed for all other record types (100, 500) ...
					modifiedLine = line +"\n";
					break;
				}
				
				out.append(modifiedLine);
			}
			
			out.flush();
			out.close();
			
			// Return the modified CSV data ...
			return outsw.toString();
		} catch (IOException e) {
			throw new RuntimeException("Unable to read data", e);
		}
	}
}
