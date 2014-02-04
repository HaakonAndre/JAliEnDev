package alien.perl.soap;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;

import javax.servlet.http.HttpServletRequest;
import javax.xml.namespace.QName;
import javax.xml.soap.MessageFactory;
import javax.xml.soap.MimeHeaders;
import javax.xml.soap.SOAPBody;
import javax.xml.soap.SOAPElement;
import javax.xml.soap.SOAPMessage;

import lazyj.Log;

/**
 * @author Alina Grigoras Class wrapper for the soap request <br />
 *         SoapRequestWrapper is able to parse a http request and extract : <br />
 *         <ul>
 *         <li>SOAP action - the name of the method called through WS</li>
 *         <li>SOAP namespace</li>
 *         <li>SOAP argument - the arguments of the method called through WS.</li>
 *         </ul>
 * 
 *         The SOAP action arguments are encapsulated into an Array of objects
 *         that can he either simple strings or hashmaps <br />
 *         Arrays are not supported because perl SOAP:Lite is not able to encode
 *         them
 */
public class SoapRequestWrapper {

	/**
	 * SOAP actionname
	 */
	private String actionName = "";

	/**
	 * @return SOAP action name
	 */
	public String getActionName() {
		return actionName;
	}

	/**
	 * @return SOAP namespace
	 */
	public String getNamespace() {
		return namespace;
	}

	/**
	 * SOAP namespace
	 */
	private String namespace = "";

	/**
	 * @return SOAP action arguments - contains the username that issued the
	 *         request, the directory from where the command was issued, the
	 *         command and its arguments
	 */
	public ArrayList<Object> getActionArguments() {
		return actionArguments;
	}

	/**
	 * SOAP action arguments - contains the username that issued the request,
	 * the directory from where the command was issued, the command and its
	 * arguments
	 */
	private final ArrayList<Object> actionArguments;

	/**
	 * builds an soap wrapper using servlet request
	 * 
	 * @param request
	 * @throws Exception
	 */
	public SoapRequestWrapper(final HttpServletRequest request) throws Exception {
		final String sSoapAction = request.getHeader("soapaction");

		final int iIndex = sSoapAction.indexOf("#");
		final int isize = sSoapAction.length();

		final String sAction = sSoapAction.substring(iIndex + 1, isize - 1);
		final String sNameSpace = sSoapAction.substring(1, iIndex);

		this.actionName = sAction;
		this.namespace = sNameSpace;

		final InputStream in = request.getInputStream();

		final MessageFactory mf = MessageFactory.newInstance();

		// being a SOAP request we have to set the headers
		final MimeHeaders mh = new MimeHeaders();
		@SuppressWarnings("unchecked")
		final Enumeration<String> e = request.getHeaderNames();

		while (e.hasMoreElements()) {
			final String s = e.nextElement();

			mh.addHeader(s, request.getHeader(s));
		}

		final SOAPMessage sm = mf.createMessage(mh, in);
		final SOAPBody sb = sm.getSOAPBody();

		final Iterator<?> itsp = sb.getChildElements(new QName(sNameSpace, sAction));

		// soap action element
		final SOAPElement seAction = (SOAPElement) itsp.next();

		// the request parameters -> this is an array of objects
		final Iterator<?> itActionParam = seAction.getChildElements();

		final ArrayList<Object> alRequestParam = new ArrayList<>();

		while (itActionParam.hasNext()) {
			final SOAPElement seParam = (SOAPElement) itActionParam.next();
			alRequestParam.add(parseSoapElement(seParam));
		}

		this.actionArguments = alRequestParam;

	}
	
	private Object parseSoapElement(final SOAPElement se) {

		if (se.getAttribute("xsi:type") != null && se.getAttribute("xsi:type").equals("xsd:string")) {
			Log.log(Log.FINEST, "SOAPElement " + se.getLocalName() + " value = " + se.getTextContent());

			// we have a string
			return se.getTextContent();
		} else if (se.getAttribute("xsi:type") != null && se.getAttribute("xsi:type").equals("xsd:int")) {
			Log.log(Log.FINEST, "SOAPElement " + se.getLocalName() + " value = " + se.getTextContent());

			// we have a string
			return se.getTextContent();
		} else {
			Log.log(Log.FINEST, "SOAPElement " + se.getLocalName() + " is a map");

			final HashMap<String, Object> hm = new HashMap<>();

			final Iterator<?> it = se.getChildElements();

			while (it.hasNext()) {

				final Object o = it.next();

				if (o instanceof SOAPElement) {
					final SOAPElement child = (SOAPElement) o;
					Log.log(Log.FINEST, "Child " + child.getLocalName());

					hm.put(child.getLocalName(), parseSoapElement(child));
				} else
					Log.log(Log.ERROR, "We didn't get a SOAPElement! This should not happen!");
			}

			return hm;
		}
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();

		sb.append("\n");
		sb.append("Action = " + this.actionName + " \n");
		sb.append("Namespace = " + this.namespace + " \n");

		for (final Object obj : this.actionArguments)
			if (obj instanceof String)
				sb.append("Argument = " + obj + " \n");
			else if (obj instanceof HashMap<?, ?>) {
				sb.append("Argument (Map) = \n");
				final HashMap<?, ?> hm = (HashMap<?, ?>) obj;

				if (hm.size() == 0)
					sb.append("Error ! HashMap null or size = 0 !!!!\n");

				sb.append(hm.toString() + "\n");

			} else
				sb.append("Unknown type");

		return sb.toString();
	}
}
