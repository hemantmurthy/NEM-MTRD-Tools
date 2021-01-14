package hamy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.URL;

public class TestPost {
	public static void main(String[] args) {
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			System.out.println("Enter Folder Name: ");
			String folder = in.readLine();
			System.out.println("Folder Name entered is " + folder);
			System.out.println("Enter Message Type: ");
			String type = in.readLine();
			System.out.println("Message Type entered is " + type);
			in.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		URL apiurl;
		try {
			apiurl = new URL("http://5ms-perf-test-client-queue.au-s1.cloudhub.io/api/hansennem/tests");
			//apiurl = new URL("http://localhost:1337");

			
		    Authenticator authenticator = new Authenticator() {

		        public PasswordAuthentication getPasswordAuthentication() {
		            return (new PasswordAuthentication("hmurt",
		                    "XXXXX".toCharArray()));
		        }
		    };
		    Authenticator.setDefault(authenticator);			
		    
		    Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("mpwebproxy1.domain.internal", 8080));
			HttpURLConnection http = (HttpURLConnection) apiurl.openConnection(proxy);
			System.out.println("Connection opened ...");
			http.setRequestMethod("POST");
			http.setDoOutput(true);
			http.setRequestProperty("Content-Type", "application/xml");
			http.setRequestProperty("transGroup", "MTRD");
			http.setRequestProperty("FileName", "HAMYTESTSUBMIT2");
			http.setRequestProperty("TopicName", "test.1");
			http.setRequestProperty("To", "ENGYAUST");
			System.out.println("Request properties set ...");
			String body = "<ase:aseXML xmlns:ase=\"urn:aseXML:r38\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"urn:aseXML:r38 http://www.nemmco.com.au/aseXML/schemas/r38/aseXML_r38.xsd\"><!--This file is the result of transformation from r36 to r38-->\r\n" + 
					"  <Header>\r\n" + 
					"    <From description=\"VECTOR ADVANCED METERING SERVICES (Australia) Pty\">VECTOMDP</From>\r\n" + 
					"    <To description=\"EnergyAustralia Pty Ltd\">ENGYAUST</To>\r\n" + 
					"    <MessageID>HAMYTESTSUBMIT2</MessageID>\r\n" + 
					"    <MessageDate>2020-07-22T04:46:12+10:00</MessageDate>\r\n" + 
					"    <TransactionGroup>MTRD</TransactionGroup>\r\n" + 
					"    <Priority>Low</Priority>\r\n" + 
					"    <SecurityContext>VECTOMDPBATCH</SecurityContext>\r\n" + 
					"    <Market>NEM</Market>\r\n" + 
					"  </Header>\r\n" + 
					"  <Transactions>\r\n" + 
					"    <Transaction transactionID=\"HAMYTESTSUBMIT2\" transactionDate=\"2020-07-22T04:46:12+10:00\">\r\n" + 
					"      <MeterDataNotification>\r\n" + 
					"        <CSVIntervalData>100,NEM12,202007220444,VECTOMDP,ENGYAUST\r\n" + 
					"200,4102379409,B1E1E2,B1,B1,N1,250111922,KWH,30,\r\n" + 
					"300,20200721,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0.040,0.374,0.535,0.123,0,0.062,0.399,0.918,0.615,1.364,1.207,0.977,0.694,0.334,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,A,,,20200722043530,\r\n" + 
					"200,4102379409,B1E1E2,E1,E1,N1,250111922,KWH,30,\r\n" + 
					"300,20200721,0.241,0.168,0.218,0.145,0.291,0.181,0.216,0.138,0.205,0.168,0.270,1.332,1.634,1.677,1.116,0.234,0,0,0.252,0.385,0.100,0,0,0.052,0,0,0,0,0.167,0.676,0.746,0.617,0.796,0.687,1.407,1.879,1.619,1.680,1.977,1.388,2.082,1.841,2.037,3.602,1.503,0.927,0.934,0.241,A,,,20200722043530,\r\n" + 
					"900</CSVIntervalData>\r\n" + 
					"        <ParticipantRole>\r\n" + 
					"          <Role>FRMP</Role>\r\n" + 
					"        </ParticipantRole>\r\n" + 
					"      </MeterDataNotification>\r\n" + 
					"    </Transaction>\r\n" + 
					"  </Transactions>\r\n" + 
					"</ase:aseXML>";
			
			OutputStream out = http.getOutputStream();
			System.out.println("Obtained OutputStream ...");
			
			byte[] bytes = body.getBytes();
			System.out.println("Number of bytes to write: " + bytes.length);
			out.write(bytes);
			out.flush();
			System.out.println("Output written ...");
			System.out.println("Getting Response ...");
			System.out.println("Response: " + http.getResponseCode());
			
			BufferedReader in = new BufferedReader(new InputStreamReader(http.getInputStream()));
			String l = null;
			while((l = in.readLine()) != null)
				System.out.println(l);
			
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
