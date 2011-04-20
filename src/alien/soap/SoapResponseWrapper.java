package alien.soap;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class SoapResponseWrapper {

	/**
	 * Called method's name
	 */
	public String methodName;
	
	/**
	 * Any response here
	 */
	public Object response;
	
	/**
	 * @return the XML-serialized response
	 */
	public String toSOAPXML(){
		final StringBuilder sb = new StringBuilder("<"+methodName+">");
		
		sb.append(toSOAPXML(response));
		
		sb.append("</"+methodName+">");
		
		return sb.toString();
	}
	
	/**
	 * @param o
	 * @return the XML serialization
	 */
	public static String toSOAPXML(final Object o){
		if (o==null)
			return null;
		
		if (o instanceof String){
			return (String) o;
		}
		
		if (o instanceof Number){
			return o.toString();
		}
		
		if (o instanceof Collection<?>){
			final StringBuilder sb = new StringBuilder("<start>");
			
			final Collection<?> c = (Collection<?>) o;
			
			for (final Object inner: c){
				sb.append(toSOAPXML(inner));
			}
			
			sb.append("</start>");
			
			return sb.toString();
		}
		
		if (o instanceof Map.Entry){
			final Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
			
			return "<key name="+toSOAPXML(entry.getKey())+" value="+toSOAPXML(entry.getValue())+"/>";
		}
		
		if (o instanceof Map<?, ?>){
			if (! (o instanceof LinkedHashMap))
				throw new IllegalArgumentException();
			
			return toSOAPXML( ((Map<?, ?>)o).entrySet() );
		}
		
		if (o instanceof SOAPXMLWriter){
			return ((SOAPXMLWriter)o).toSOAPXML();
		}
		
		throw new IllegalArgumentException("Unknown type : "+o.getClass().getCanonicalName());
	}
	
}
