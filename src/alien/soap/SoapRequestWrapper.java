package alien.soap;

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
 * @author Alina Grigoras
 * Class wrapper for the soap request <br />
 * SoapRequestWrapper is able to parse a http request and extract : <br />
 * <ul>
 * 		<li>SOAP action - the name of the method called through WS</li>
 * 		<li>SOAP namespace</li>
 * 		<li>SOAP argument - the arguments of the method called through WS. </li>
 * </ul>
 * 
 * The SOAP action arguments are encapsulated into an Array of objects 
 * that can he either simple strings or hashmaps <br />
 * Arrays are not supported because perl SOAP:Lite is not able to encode them
 */
public class SoapRequestWrapper {

	/**
	 * 
	 */
	private String actionName = "";

	/**
	 * @return
	 */
	public String getActionName() {
		return actionName;
	}

	/**
	 * @return
	 */
	public String getNamespace() {
		return namespace;
	}

	/**
	 * 
	 */
	private String namespace = "";


	/**
	 * @return
	 */
	public ArrayList<Object> getActionArguments() {
		return actionArguments;
	}

	/**
	 * 
	 */
	private ArrayList<Object> actionArguments;

	
	/**
	 * builds an soap wrapper using servlet request
	 * @param request
	 * @throws Exception
	 */
	public SoapRequestWrapper(HttpServletRequest request) throws Exception{
		String sSoapAction = request.getHeader("soapaction");

		int iIndex = sSoapAction.indexOf("#");
		int isize = sSoapAction.length();

		String sAction = sSoapAction.substring(iIndex+1, isize-1);
		String sNameSpace = sSoapAction.substring(1, iIndex);

		this.actionName = sAction;
		this.namespace = sNameSpace;


		InputStream in = request.getInputStream();

		MessageFactory mf = MessageFactory.newInstance();

		//being a SOAP request we have to set the headers
		MimeHeaders mh = new MimeHeaders();
		Enumeration<String> e = request.getHeaderNames();

		while(e.hasMoreElements()){
			String s = e.nextElement();

			mh.addHeader(s, request.getHeader(s));
		}

		SOAPMessage sm = mf.createMessage(mh, in);
		SOAPBody sb = sm.getSOAPBody();
	
		Iterator<?> itsp =  sb.getChildElements(new QName(sNameSpace,sAction));
		
		//soap action element
		SOAPElement seAction = (SOAPElement) itsp.next();
		
		//the request parameters -> this is an array of objects
		Iterator<?> itActionParam = seAction.getChildElements();

		ArrayList<Object> alRequestParam = new ArrayList<Object>();
		
		while(itActionParam.hasNext()){
			SOAPElement seParam = (SOAPElement) itActionParam.next();
			alRequestParam.add(parseSoapElement(seParam));		
		}	
		
		
		this.actionArguments = alRequestParam;
		
	}
	
	/**
	 * @param se
	 * @return
	 */
	private Object parseSoapElement(SOAPElement se){
		
		if(se.getAttribute("xsi:type") != null && se.getAttribute("xsi:type").equals("xsd:string")){
			Log.log(Log.INFO, "SOAPElement "+se.getLocalName()+" value = "+se.getTextContent());
			
			//we have a string
			return se.getTextContent();
		}
		else if (se.getAttribute("xsi:type") != null && se.getAttribute("xsi:type").equals("xsd:int")){
			Log.log(Log.INFO, "SOAPElement "+se.getLocalName()+" value = "+se.getTextContent());
			
			//we have a string
			return se.getTextContent();			
		}
		else{
			Log.log(Log.INFO, "SOAPElement "+se.getLocalName()+" is a map");
			
			HashMap<String, Object> hm = new HashMap<String, Object>();
			
			
			Iterator<SOAPElement> it = se.getChildElements();
		
			while(it.hasNext()){
				
				SOAPElement child = (SOAPElement) it.next();
				Log.log(Log.INFO, "Child "+child.getLocalName());				
				hm.put(child.getLocalName(), parseSoapElement(child));
			}
			
			return hm;
		}
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("\n");
		sb.append("Action = "+this.actionName+" \n");
		sb.append("Namespace = "+this.namespace+" \n");
		
		for (Object obj: this.actionArguments){
			
			if(obj instanceof String){
				sb.append("Argument = "+obj+" \n");
			}
			else if (obj instanceof HashMap<?, ?>){
				HashMap<?, ?> hm = (HashMap<?, ?>) obj;
				
				if(hm == null || hm.size() == 0)
					sb.append("Error ! HashMap null or size = 0 !!!!\n");
				
				sb.append(hm.toString()+"\n");
				
			}
			else{
				sb.append("Unknown type");
			}
		}
		
		return sb.toString();
	}
}
