package alien.protocols;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.zip.GZIPInputStream;


public class HTTP {

	final private int timeOut = 60;
	final private int BUFFER_LEN_STRING = 80;
	
	private URLConnection lastURLConnection;
	private boolean forceDefaultEncoding = false;
	private String defaultEncoding = "ISO-8859-1";
	
	private InputStream prepareInputStream(String urlToRetrieve) throws IOException
	{
		URL url = new URL(urlToRetrieve);
		URLConnection uc = url.openConnection();
		if (timeOut > 0)
		{
			uc.setConnectTimeout(timeOut);
			uc.setReadTimeout(timeOut);
		}
		InputStream is = uc.getInputStream();
		// deflate, if necesarily
		if ("gzip".equals(uc.getContentEncoding()))
			is = new GZIPInputStream(is);

		lastURLConnection = uc;
		return is;
	}
	// detects encoding associated to the current URL connection, taking into account the default encoding
	public String detectEncoding()
	{
		if (forceDefaultEncoding)
			return defaultEncoding;
		String detectedEncoding = detectEncodingFromContentTypeHTTPHeader(lastURLConnection.getContentType());
		if (detectedEncoding == null)
			return defaultEncoding;

		return detectedEncoding;
	}


	public static String detectEncodingFromContentTypeHTTPHeader(String contentType)
	{
		if (contentType != null)
		{
			int chsIndex = contentType.indexOf("charset=");
			if (chsIndex != -1)
			{
				String enc = contentType.substring(contentType.indexOf("charset="));
				if(enc.indexOf(';') != -1)
					enc = enc.substring(enc.indexOf(";"));
				return enc.trim();
			}
		}
		return null;
	}


	// retrieves into an String object
	public String retrieve(String urlToRetrieve)
	throws MalformedURLException , IOException
	{
		InputStream is = prepareInputStream(urlToRetrieve);
		String encoding = detectEncoding();
		BufferedReader in = new BufferedReader(new InputStreamReader(is , encoding));
		StringBuilder output = new StringBuilder(BUFFER_LEN_STRING);
		String str;
		boolean first = true;
		while ((str = in.readLine()) != null)
		{
			if (!first)
				output.append("\n");
			first = false;
			output.append(str);
		}
		in.close();
		return output.toString();
	}
}
