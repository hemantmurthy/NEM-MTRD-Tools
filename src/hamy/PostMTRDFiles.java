package hamy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Console;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.ProtocolException;
import java.net.Proxy;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

/**
 * Program to post aseXML files from a folder using the API
 * @author Hemant Murthy
 *
 */
public class PostMTRDFiles {
	private static final DocumentBuilderFactory DOCUMENT_BUILDER_FACTORY = DocumentBuilderFactory.newInstance();
	private static final XPathFactory XPATH_FACTORY = XPathFactory.newInstance();
	private static final DateTimeFormatter CONSOLE_DATE_FORMAT = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
	private static final DateTimeFormatter LOG_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss");
	private static final DateTimeFormatter FILE_NAME_SUFFIX_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
	
	private String apiUrl = null;
	private String destinationName = null;
	private boolean isProxyRequired = false;
	private String proxyHost = null;
	private int proxyPort = 0;
	private String proxyUserName = null;
	private String proxyPassword = null;
	private boolean mustPost = false;
	
	private List<String> transactionTypes = new ArrayList<String>();
	private String sourceFolder = ".";
	
	private int numPermits = 5;
	private Semaphore permits = null;
	
	private int numNEM13ToBePosted = 0;
	private int numNEM12ToBePosted = 0;
	private int numTACKToBePosted = 0;
	private int numNEM13Skipped = 0;
	private int numNEM12Skipped = 0;
	private int numTACKSkipped = 0;
	
	private int numFilesPosted = 0;
	private int numFilesFailed = 0;
	private int numFilesSkipped = 0;
	
	private File successLogFile = null;
	private BufferedWriter successLogger = null;
	private File errorLogFile = null;
	private BufferedWriter errorLogger = null;
	
	private class MTRDFile {
		public String type = "";
		public File file = null;
		public String messageId = "";
		public String from = "";
		public String to = "";
		public String role = "";
		public String transactionId = "";
		public int numTransactions;
		public int numMeters;
		public int numReads;
	}
	
	List<MTRDFile> filesToBeProcessed = new ArrayList<>();
	
	public static void main(String[] a) {
		PostMTRDFiles app = new PostMTRDFiles();
		try {
			app.init();
			app.buildFileList();
			if(app.confirm()) 
				app.start();
			else
				System.out.println("No files posted.");
		} finally {
			app.cleanUp();
		}
	}
	
	/**
	 * Initialize the program by setting parameters for the run.
	 */
	private void init() {
		try {
			// Defaults ...
			transactionTypes.add("NEM12");
			transactionTypes.add("NEM13");
			transactionTypes.add("TACK");
			
			apiUrl = "http://localhost:1337";
			destinationName = "ea.nem.all.external.nemgatekeeper.interface.in";
			isProxyRequired = false;
			//proxyHost = "mpwebproxy1.domain.internal";
			//proxyPort = 8080;
			numPermits = 5;
			
			// Get parameters from user ...
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			
			// API URL ...
			System.out.print("Enter API URL (" + apiUrl + "): ");
			String value = in.readLine();
			if(!value.trim().equals("")) {
				apiUrl = value;
			}

			// JMS Destination ...
			System.out.print("Enter JMS Destination (" + (destinationName == null ? "not set" : destinationName) + "): ");
			value = in.readLine();
			if(!value.trim().equals("")) {
				destinationName = value.trim();
			}

			// Source Folder ...
			System.out.print("Enter source folder ('" + (sourceFolder == null ? "not set" : sourceFolder) + "'): ");
			value = in.readLine();
			if(!value.trim().equals("")) {
				sourceFolder = value;
			}
			
			// HTTP Proxy ... 
			System.out.print("Will connection through a proxy be required? (" + (isProxyRequired ? "Y": "N") + "): ");
			value = in.readLine();
			
			if(!value.trim().equals("") && value.trim().equalsIgnoreCase("y")) {
				isProxyRequired = true;
		
				System.out.print("Enter proxy server host (" + (proxyHost == null ? "not set" : proxyHost) + "): ");
				value = in.readLine();
			
				if(!value.trim().equals("")) {
					proxyHost = value;
				}
				
				System.out.print("Enter proxy server port (" + proxyPort + "): ");
				value = in.readLine();
			
				if(!value.trim().equals("")) {
					proxyPort = Integer.parseInt(value);
				}
				
				System.out.print("Enter proxy user name (" + (proxyUserName == null ? "not set" : proxyUserName) + "): ");
				value = in.readLine();
			
				if(!value.trim().equals("")) {
					proxyUserName = value;
				}
				
				System.out.print("Enter proxy password: ");
				value = in.readLine();
			
				if(!value.trim().equals("")) {
					proxyPassword = value;
				}
				
			} else if(value.trim().equals("")) {
				// Retail default ...
			} else {
				isProxyRequired = false;
				proxyHost = null;
				proxyPort = 0;
				proxyUserName = null;
				proxyPassword = null;
			}
			
			// Number of threads ...
			System.out.print("Number of threads ('" + numPermits + "'): ");
			value = in.readLine();
			if(!value.trim().equals("")) {
				numPermits = Integer.parseInt(value);
			}
			
			// Transaction types to process ...
			System.out.print("Transaction types to process (");
			for(String t : transactionTypes) System.out.print(" " + t);
			System.out.print(" ): ");
			value = in.readLine();
			if(!value.trim().equals("")) {
				transactionTypes.clear();
				for(String t : value.trim().split(" "))
					transactionTypes.add(t.trim());
			}
			
			String suffix = FILE_NAME_SUFFIX_FORMAT.format(LocalDateTime.now());
			createSuccessLogFile(suffix);
			createErrorLogFile(suffix);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Analyse the specified folder and build the list of MTRD files to be posted.
	 */
	private void buildFileList() {
		long startTime = System.currentTimeMillis();
		System.out.println("Time: " + CONSOLE_DATE_FORMAT.format(LocalDateTime.now()));
		
		System.out.println("************** Analysing Files ***************");
		File folder = new File(sourceFolder);
		System.out.println("Reading files in folder: " + folder.getAbsolutePath());
		File[] filesInFolder = folder.listFiles();
		if(filesInFolder == null) filesInFolder = new File[0];
		System.out.println("Total number of entries in folder: " + filesInFolder.length);
		
		int numFilesInFolder = 0;
		for(File file : filesInFolder) {
			if(!file.isDirectory()) {
				++numFilesInFolder;
				FileAnalyser analyser = new FileAnalyser(file);
				analyser.analyse();
			}
		}
		
		System.out.println("Time: " + CONSOLE_DATE_FORMAT.format(LocalDateTime.now()));
		System.out.println("********** Files Analysis Completed ***********");
		System.out.println("Time taken : " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
		System.out.println("\nNumber of files in folder: " + numFilesInFolder);
		System.out.println("Number of files to be posted: " + filesToBeProcessed.size());
		System.out.println("Number of files to be skipped: " + numFilesSkipped);
		System.out.println("\nBreak up of MTRD files in folder ...");
		System.out.println("NEM12 " + numNEM12ToBePosted + ", skipping " + numNEM12Skipped);
		System.out.println("NEM13 " + numNEM13ToBePosted + ", skipping " + numNEM13Skipped);
		System.out.println("TACK " + numTACKToBePosted + ", skipping " + numTACKSkipped);
	}
	
	private boolean confirm() {
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		
		// API URL ...
		try {
			System.out.print("Do you want to post files? (Y)es / (N)o / (A)nalyse only: ");
			String value = in.readLine();
			if(value.trim().equalsIgnoreCase("Y")) {
				mustPost = true;
				return true;
			} else if (value.trim().equalsIgnoreCase("A")) {
				mustPost = false;
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	/**
	 * Start the posting process
	 */
	private void start() {
		long startTime = System.currentTimeMillis();
		File folder = new File(sourceFolder);
		System.out.println("Reading files in folder: " + folder.getAbsolutePath());
		File[] filesInFolder = folder.listFiles();
		if(filesInFolder == null) filesInFolder = new File[0];
		
		System.out.println("Time: " + CONSOLE_DATE_FORMAT.format(LocalDateTime.now()));
		System.out.println("********** Files Posting Started ********\n");
		
		permits = new Semaphore(numPermits);
		
		for(MTRDFile mtrdFile : filesToBeProcessed) {
			try {
				// Check if thread are available or wait for one to be free ...
				permits.acquire();
				FileProcessor processor = new FileProcessor(mtrdFile);
				processor.process();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch(Exception e) {
				logPostError(mtrdFile, e.getMessage());
			}
		}
		
		try {
			// Wait for all threads to complete ...
			permits.acquire(numPermits);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("\n********** Files Posting Completed **********");
		System.out.println("Time: " + CONSOLE_DATE_FORMAT.format(LocalDateTime.now()));
		System.out.println("Time taken : " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
		System.out.println("Num. files posted: " + numFilesPosted);
		System.out.println("Num. files failed: " + numFilesFailed);
		System.out.println("Num. files skipped: " + numFilesSkipped);
	}
	
	private synchronized void logPostSuccess(MTRDFile mtrdFile, int size, int returnCode, boolean isExpectedResponse, long durationToPost) {
		++numFilesPosted;
		System.out.println(numFilesPosted + ":> File " + mtrdFile.file.getName() + ", Message ID " + mtrdFile.messageId + " posted. Response Code: " + returnCode);
		try {
			successLogger.write(LOG_DATE_FORMAT.format(LocalDateTime.now()) + "," + mtrdFile.type + "," + mtrdFile.file.getName() + "," + size + "," + mtrdFile.messageId + "," + mtrdFile.from + "," + 
						mtrdFile.to + "," + mtrdFile.role + "," + mtrdFile.transactionId + "," + mtrdFile.numTransactions + "," + mtrdFile.numMeters + "," + mtrdFile.numReads + "," + returnCode + "," + isExpectedResponse + "," + durationToPost);
			successLogger.newLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private synchronized void logPostError(MTRDFile mtrdFile, String errorMessage) {
		++numFilesFailed;
		try {
			errorLogger.write("ERROR," + mtrdFile.type + "," + mtrdFile.file.getName() + "," + errorMessage);
			errorLogger.newLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private synchronized void logPostSkipped(String fileName, String reason) {
		++numFilesSkipped;
		try {
			errorLogger.write("SKIPPED," + fileName + "," + reason);
			errorLogger.newLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private synchronized void createSuccessLogFile(String suffix) {
		if(suffix != null && !"".contentEquals(suffix))
			successLogFile = new File("success-" + suffix + ".log");
		else successLogFile = new File("success.log");
		try {
			if(successLogFile.createNewFile())
				System.out.println("Success log file created. File path: " + successLogFile.getAbsolutePath());
			else
				System.out.println("Appending to existing Success log file. File path: " + successLogFile.getAbsolutePath());
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		try {
			successLogger = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(successLogFile)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private synchronized void createErrorLogFile(String suffix) {
		if(suffix != null && !"".contentEquals(suffix))
			errorLogFile = new File("errors-" + suffix + ".log");
		else errorLogFile = new File("errors.log");
		try {
			if(errorLogFile.createNewFile())
				System.out.println("Errors log file created. File path: " + errorLogFile.getAbsolutePath());
			else
				System.out.println("Appending to existing Errors log file. File path: " + errorLogFile.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			errorLogger = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(errorLogFile)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void cleanUp() {
		try {
			if(successLogger != null) {
				successLogger.flush();
				successLogger.close();
			}
			
			if(errorLogger != null) {
				errorLogger.flush();
				errorLogger.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	private static final Pattern nem12_200_rec = Pattern.compile("(?m)^200");
	private static final Pattern nem12_300_rec = Pattern.compile("(?m)^300");
	private static final Pattern nem12_250_rec = Pattern.compile("(?m)^250");
	
	/**
	 * Analyse a file and create a record to be posted
	 * @author HMURTH
	 *
	 */
	class FileAnalyser {
		private Document fileDoc = null;
		private File file = null;
		
		public FileAnalyser(File file) {
			this.file = file;
		}
		
		public void analyse() {
			try {
				parseFile();

				switch(getFileType(file)) {
				case "NEM12": 
					if(transactionTypes.contains("NEM12")) {
						MTRDFile nem12file = getNEM12FileParameters(this.file);
						// Filter further if necessary here
						
						if(addFileToBeProcessed(nem12file))
							++numNEM12ToBePosted;
						else {
							++numNEM12Skipped;
							logPostSkipped(file.getName(), "Unable to build NEM12 post request");						
						}
					} else {
						++numNEM12Skipped;
						logPostSkipped(file.getName(), "Skipping NEM12");
					}
					break;
				case "NEM13":
					if(transactionTypes.contains("NEM13")) {
						MTRDFile nem13file = getNEM13FileParameters(this.file);
						// Filter further if necessary here

						if(addFileToBeProcessed(nem13file))
							++numNEM13ToBePosted;
						else {
							++numNEM13Skipped;
							logPostSkipped(file.getName(), "Unable to build NEM13 post request");						
						}
					} else {
						++numNEM13Skipped;
						logPostSkipped(file.getName(), "Skipping NEM13");
					}
					break;
				case "TACK":
					if(transactionTypes.contains("TACK")) {
						MTRDFile tackfile = getTACKFileParameters(this.file);
						// Filter further if necessary here
						
						if(addFileToBeProcessed(tackfile))
							++numTACKToBePosted;
						else {
							++numTACKSkipped;
							logPostSkipped(file.getName(), "Unable to build TACK post request");						
						}
					} else {
						++numTACKSkipped;
						logPostSkipped(file.getName(), "Skipping TACK");
					}
					break;
				case "UNKNOWN":
				default:
					logPostSkipped(file.getName(), "Skipping. Unknown File Type");
					break;
				}
			} catch (Exception e) {
				logPostSkipped(file.getName(), "Unable to build file post request");
			}
		}
		
		private boolean addFileToBeProcessed(MTRDFile mtrdFile) {
			if(mtrdFile != null) { 
				filesToBeProcessed.add(mtrdFile);
				return true;
			}
			
			return false;
		}
		
		private void parseFile() {
			try {
				DocumentBuilder db;
				db = DOCUMENT_BUILDER_FACTORY.newDocumentBuilder();
				fileDoc = db.parse(this.file);
			} catch(ParserConfigurationException | IOException | SAXException e) {
				throw new RuntimeException("Cannot parse file", e);
			}
		}
		
		private String getFileType(File file) {
			try {
				XPath xp = XPATH_FACTORY.newXPath();
				if((Boolean) xp.evaluate("string(aseXML/Header/TransactionGroup) = 'MTRD' and boolean(aseXML/Transactions/Transaction/MeterDataNotification/CSVIntervalData)", fileDoc, XPathConstants.BOOLEAN))
					return "NEM12";
				else if((Boolean) xp.evaluate("string(aseXML/Header/TransactionGroup) = 'MTRD' and boolean(aseXML/Transactions/Transaction/MeterDataNotification/CSVConsumptionData)", fileDoc, XPathConstants.BOOLEAN))
					return "NEM13";
				else if((Boolean) xp.evaluate("string(aseXML/Header/TransactionGroup) = 'MTRD' and boolean(aseXML/Acknowledgements/TransactionAcknowledgement)", fileDoc, XPathConstants.BOOLEAN))
					return "TACK";
				else return "UNKNOWN";
			} catch (XPathExpressionException e) {
			}
			return "UNKNOWN";
		}
		
		private MTRDFile getNEM12FileParameters(File file) {
			MTRDFile mtrdFile = new MTRDFile();
			mtrdFile.file = file;
			mtrdFile.type = "NEM12";
			setCommonFileParameters(mtrdFile);
			setNEM12Parameters(mtrdFile);
			return mtrdFile;
		}

		private MTRDFile getNEM13FileParameters(File file) {
			MTRDFile mtrdFile = new MTRDFile();
			mtrdFile.file = file;
			mtrdFile.type = "NEM13";
			setCommonFileParameters(mtrdFile);
			setNEM13Parameters(mtrdFile);
			return mtrdFile;
		}
		
		private MTRDFile getTACKFileParameters(File file) {
			MTRDFile mtrdFile = new MTRDFile();
			mtrdFile.file = file;
			mtrdFile.type = "TACK";
			setCommonFileParameters(mtrdFile);
			setTACKParameters(mtrdFile);
			return mtrdFile;
		}

		private void setCommonFileParameters(MTRDFile mtrdFile) {
			try {
				XPath xp = XPATH_FACTORY.newXPath();
				mtrdFile.messageId = (String) xp.evaluate("aseXML/Header/MessageID", fileDoc, XPathConstants.STRING);
				mtrdFile.to = (String) xp.evaluate("aseXML/Header/To", fileDoc, XPathConstants.STRING);
				mtrdFile.from = (String) xp.evaluate("aseXML/Header/From", fileDoc, XPathConstants.STRING);
				
				List<String> roles = new ArrayList<>();
				List<String> transactionIds = new ArrayList<>();
				
				int numTransactions = 0;
				NodeList transNodes = (NodeList) xp.evaluate("aseXML/Transactions/Transaction", fileDoc, XPathConstants.NODESET);
				String r = null, tranId;
				for(int i = 0; i < transNodes.getLength(); ++i) {
					++numTransactions;
					Node tranNode = transNodes.item(i);
					r = (String) xp.evaluate("MeterDataNotification/ParticipantRole/Role", tranNode, XPathConstants.STRING);
					r = r != null ? r.trim() : "";
					if(!"".equals(r) && !roles.contains(r))
						roles.add(r);
					
					tranId = (String) xp.evaluate("@transactionID", tranNode, XPathConstants.STRING);
					tranId = tranId != null ? tranId.trim() : "";
					if(!"".equals(tranId) && !transactionIds.contains(tranId))
						transactionIds.add(tranId);
				}
				
				mtrdFile.numTransactions = numTransactions;
				
				if(roles.size() <= 0) mtrdFile.role = "";
				else if(roles.size() > 1) mtrdFile.role = "MULTIPLE";
				else mtrdFile.role = roles.get(0);
				
				if(transactionIds.size() <= 0) mtrdFile.transactionId = "";
				else if(transactionIds.size() > 1) mtrdFile.transactionId = "<<<MULTIPLE>>>";
				else mtrdFile.transactionId = transactionIds.get(0);
				
			} catch (XPathExpressionException e) {
				throw new RuntimeException("Unable to set common post parameters", e);
			}
		}
		
		private void setNEM12Parameters(MTRDFile mtrdFile) {
			try {
				XPath xp = XPATH_FACTORY.newXPath();
				int numMeters = 0;
				int numReads = 0;
				
				NodeList intDataNodes = (NodeList) xp.evaluate("aseXML/Transactions/Transaction/MeterDataNotification/CSVIntervalData", fileDoc, XPathConstants.NODESET);
				String csvdata = null;
				Matcher match200 = null, match300 = null;
				
				for(int i = 0; i < intDataNodes.getLength(); ++i) {
					csvdata = intDataNodes.item(i).getTextContent();
					match200 = nem12_200_rec.matcher(csvdata);
					while(match200.find()) 
						++numMeters;
					
					match300 = nem12_300_rec.matcher(csvdata);
					while(match300.find())
						++numReads;
				}
				
				mtrdFile.numMeters = numMeters;
				mtrdFile.numReads = numReads;
			} catch (XPathExpressionException e) {
				throw new RuntimeException("Unable to set NEM12 post parameters", e);
			}
		}

		private void setNEM13Parameters(MTRDFile mtrdFile) {
			try {
				XPath xp = XPATH_FACTORY.newXPath();
				int numMeters = 0;
				int numReads = 0;
				
				NodeList consDataNodes = (NodeList) xp.evaluate("aseXML/Transactions/Transaction/MeterDataNotification/CSVConsumptionData", fileDoc, XPathConstants.NODESET);
				String csvdata = null;
				Matcher match250 = null;
				
				for(int i = 0; i < consDataNodes.getLength(); ++i) {
					csvdata = consDataNodes.item(i).getTextContent();
					match250 = nem12_250_rec.matcher(csvdata);
					while(match250.find()) 
						++numMeters;
					
					numReads = numMeters;
				}
				
				mtrdFile.numMeters = numMeters;
				mtrdFile.numReads = numReads;
			} catch (XPathExpressionException e) {
				throw new RuntimeException("Unable to set NEM13 post parameters", e);
			}
		}
		
		private void setTACKParameters(MTRDFile mtrdFile) {
			try {
				XPath xp = XPATH_FACTORY.newXPath();
				int numTransactions = 0;
				NodeList transNodes = (NodeList) xp.evaluate("aseXML/Acknowledgements/TransactionAcknowledgement", fileDoc, XPathConstants.NODESET);
				for(int i = 0; i < transNodes.getLength(); ++i) {
					++numTransactions;
				}
				
				mtrdFile.numTransactions = numTransactions;
			} catch (XPathExpressionException e) {
				throw new RuntimeException("Unable to set TACK post parameters", e);
			}
		}

	}
	
	class FileProcessor implements Runnable {
		private Thread thread = null;
		private MTRDFile mtrdFile = null;
		
		public FileProcessor(MTRDFile mtrdFile) {
			this.mtrdFile = mtrdFile;
		}
		
		public void process() {
			thread = new Thread(this);
			thread.start();
		}
		
		@Override
		public void run() {
			MTRDProcessor processor = null;
			switch(mtrdFile.type) {
			case "NEM12":
			case "NEM13":
			case "TACK":
				System.out.println("Processing " + mtrdFile.type + ", " + mtrdFile.file.getName());
				processor = new MTRDProcessor();
				processor.process(mtrdFile);
				break;
			default:
				System.out.println("Ignoring file " + mtrdFile.file.getName());
				break;
			}
			permits.release();
		}
	}
	
	class MTRDProcessor {
		private MTRDFile mtrdFile = null;
		public MTRDProcessor() {
		}
		
		public void process(MTRDFile mtrdFile) {
			this.mtrdFile = mtrdFile;
			try {		
				post();
			} catch(RuntimeException e) {
				logPostError(mtrdFile, e.getMessage());
				System.out.println("Unable to process file " + mtrdFile.file.getName());
			}
		}
		
		private void post() {
			HttpURLConnection connection = getConnection();
			if(connection != null) {
				try {
					if(mustPost) {
						long startTime = System.currentTimeMillis();
						connection.setRequestMethod("POST");
						connection.setDoOutput(true);
						connection.setDoInput(true);
						connection.setRequestProperty("Content-Type", "application/xml");
						connection.setRequestProperty("transGroup", "MTRD");
						connection.setRequestProperty("TopicName", destinationName);
						connection.setRequestProperty("FileName", mtrdFile.messageId);
						connection.setRequestProperty("To", mtrdFile.to);
						
						BufferedWriter cout = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream()));
						BufferedReader fin = new BufferedReader(new FileReader(mtrdFile.file));
						String buf = null; int size = 0;
						while((buf = fin.readLine()) != null) {
							cout.write(buf);
							cout.newLine();
							size += buf.length() + 1;
						}
						--size;
						fin.close();
						
						cout.flush();
						cout.close();
						int respCode = connection.getResponseCode();
						if(respCode < 200 || respCode > 299)
							throw new RuntimeException("Error response code received. " + respCode);
						
						StringBuffer resp = new StringBuffer();
						BufferedReader cin = new BufferedReader(new InputStreamReader(connection.getInputStream()));
						while((buf = cin.readLine()) != null)
							resp.append(buf);
						
						long duration = (System.currentTimeMillis() - startTime) / 1000;
						
						boolean isExpectedResponse = resp.toString().equals("{  \"publish\": \"true\"}");
						
						logPostSuccess(mtrdFile, size, respCode, isExpectedResponse, duration);
					} else {
						BufferedReader fin = new BufferedReader(new FileReader(mtrdFile.file));
						String buf = null; int size = 0;
						while((buf = fin.readLine()) != null) {
							size += buf.length() + 1;
						}
						--size;
						fin.close();
						
						logPostSuccess(mtrdFile, size, 0, false, 0L);
					}
				} catch (ProtocolException e) {
					throw new RuntimeException(e);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
		
		private HttpURLConnection getConnection() {
			URL url;
			try {
				url = new URL(apiUrl);
			} catch (MalformedURLException e1) {
				e1.printStackTrace();
				return null;
			}
			
			HttpURLConnection httpConnection = null;
			try {
			    if(isProxyRequired) {
					if(proxyUserName != null && !"".equals(proxyUserName) && proxyPassword != null && !"".equals(proxyPassword)) {
				    	Authenticator authenticator = new Authenticator() {
					    	@Override
							public PasswordAuthentication getPasswordAuthentication() {
							    return (new PasswordAuthentication(proxyUserName, proxyPassword.toCharArray()));
					    	}
						};
					    Authenticator.setDefault(authenticator);	
					}
			    
					Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
						httpConnection = (HttpURLConnection) url.openConnection(proxy);
				} else {
					httpConnection = (HttpURLConnection) url.openConnection();
				}
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
			
			return httpConnection;
		}
	}
}
