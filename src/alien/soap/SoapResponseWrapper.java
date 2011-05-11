package alien.soap;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Alina Grigoras
 * Class wrapper able to serialize soap response <br />
 * SOAP response needs: 
 * 	<ul>
 * 		<li>action name - SOAP action or the name of the method called through the WS</li>
 * 		<li>namespage - SOAP Namespace</li>
 * 		<li>SOAP object response - response object that will be serialized. The class is able to serialize 
 * 			only combinations of Strings, Arrays and Maps, but offers an iterface to implement your own object serialization
 * 		</li>
 */
public class SoapResponseWrapper {

	/**
	 * Called method's name
	 */
	private String actionName = "";

	/**
	 * XML namespace
	 */
	private String namespace = "";

	/**
	 * Any response here
	 */
	private Object response = "";
	
	/**
	 * @param sActionName - SOAP action name or the name of the method called through WS
	 * @param sNamespace - SOAP namespace
	 * @param oResponse -  response object that will be serialized. The class is able to serialize 
 * 			only combinations of Strings, Arrays and Maps, but offers an iterface to implement your own object serialization
	 */
	public SoapResponseWrapper(String sActionName, String sNamespace, Object oResponse){
	
		if(sActionName == null || sActionName.equals(""))
			throw new NullPointerException(
			"No soap action! Please fill SoapResponseWrapper");
			
		if(sNamespace == null || sNamespace.equals(""))
			throw new NullPointerException(
			"No namespace! Please fill SoapResponseWrapper");	
		
		if (oResponse == null)
			throw new NullPointerException(
			"SOAP Object is null! Please fill the object");
		
		this.actionName = sActionName;
		this.namespace = sNamespace;
		this.response = oResponse;
	}

	/**
	 * @return the XML-serialized response
	 */
	public String toSOAPXML() {
	
		if(actionName == null || actionName.equals(""))
			throw new NullPointerException(
			"No soap action! Please fill SoapResponseWrapper");
			
		if(namespace == null || namespace.equals(""))
			throw new NullPointerException(
			"No namespace! Please fill SoapResponseWrapper");	
		
		if (response == null)
			throw new NullPointerException(
			"SOAP Object is null! Please fill the object");

		final StringBuilder sb = new StringBuilder();

		sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		sb.append("<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:soapenc=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" soap:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">\n");
		sb.append("		<soap:Body>\n");
		sb.append("			<ns1:"
				+ actionName
				+ "Response xmlns:ns1=\"urn:"
				+ namespace
				+ "\">\n");

		sb.append(toSOAPXML(response));

		sb.append("			</ns1:" + actionName + "Response>\n");
		sb.append("		</soap:Body>\n");
		sb.append("</soap:Envelope>\n");

		return sb.toString();
	}

	/**
	 * @param Object 
	 * @return the serialization of one soap element
	 */
	private static String toSOAPXMLElement(Object o) {
		if (o instanceof String)
			return (String) o;

		if (o instanceof Number)
			return o.toString();

		if (o instanceof Collection<?>) {
			final Collection<?> c = (Collection<?>) o;

			StringBuilder sb = new StringBuilder();
			
			for (final Object inner : c) {
				if(inner instanceof Collection<?>){
					final Collection<?> c1 = (Collection<?>) inner;
						sb.append("<item "+getXsiType(inner)+" soapenc:arrayType=\"xsd:string["+c1.size()+"]\">" + toSOAPXMLElement(inner)+ "</item>");
				}
				else
					sb.append("<item "+getXsiType(inner)+">" + toSOAPXMLElement(inner)+ "</item>");
			}
		
			return sb.toString();
		}

		if(o instanceof Map<?, ?>){
			Map<?, ?> m = (Map<?, ?>) o;
			Set<?> s = m.entrySet();

			StringBuilder sb = new StringBuilder();

			for (final Object inner: s){
				Entry<?, ?> e = (Entry<?, ?>) inner;

				String sKey = (String) e.getKey();
				Object oValue = e.getValue();
				
				if(oValue instanceof Collection<?>){
					final Collection<?> c = (Collection<?>) oValue;
					sb.append("<"+sKey+" "+getXsiType(oValue)+" soapenc:arrayType=\"xsd:string["+c.size()+"]\">");
				}
				else
					sb.append("<"+sKey+" "+getXsiType(oValue)+">");
				
				sb.append(toSOAPXMLElement(oValue));
				sb.append("</"+sKey+">");
			}

			return sb.toString();
		}

		if (o instanceof SOAPXMLWriter) {
			return ((SOAPXMLWriter) o).toSOAPXML();
		}

		throw new IllegalArgumentException("Unknown type : "
				+ o.getClass().getCanonicalName());
	}

	/**
	 * @param o
	 * @return the XML serialization
	 */
	private static String toSOAPXML(final Object o) {
		if (o == null)
			throw new NullPointerException(
			"SOAP Object is null! Please fill the object");

		return "				<return xmlns:ns2=\"http://schemas.xmlsoap.org/soap/encoding/\" "+getXsiType(o)+">"+toSOAPXMLElement(o)+"</return>\n";

	}


	/**
	 * @param Object
	 * @return SOAP element type
	 */
	private static String getXsiType(final Object o) {
		if (o instanceof String){
			return "xsi:type=\"xsd:string\"";
		}

		if(o instanceof Number){
			if(o instanceof Integer)
				return "xsi:type=\"xsd:int\"";
			else
				return "xsi:type=\"xsd:string\"";
		}

		if(o instanceof Collection<?>){
			return "xsi:type=\"soapenc:Array\"";
		}

		if(o instanceof Map<?, ?>){
			return "xsi:type=\"soapenc:Struct\"";
		}

		if(o instanceof SOAPXMLWriter){
			return "xsi:type=\"soapenc:Struct\"";
		}

		throw new IllegalArgumentException("Unknown type : "
				+ o.getClass().getCanonicalName());
	}

}
