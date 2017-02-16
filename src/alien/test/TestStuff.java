package alien.test;

import javax.naming.NamingException;

import alien.site.ComputingElement;

public class TestStuff {

	public static void main(String[] args) throws NamingException {

		System.out.println("Starting");

		// Set<String> defaultQos = LDAPHelper.checkLdapInformation("(objectClass=AliEnVOConfig)", "ou=Config,", "sedefaultQosandCount");
		// System.out.println(defaultQos);

		// final HashMap<String, Object> sites = LDAPHelper.checkLdapTree("(&(name=CERN-AURORA))", "ou=CE,ou=Services,ou=CERN,ou=Sites,");
		// final HashMap<String, Object> sites = LDAPHelper.checkLdapTree("(&(host=voboxalice1.cern.ch))", "ou=Config,ou=CERN,ou=Sites,");
		//
		// if (sites != null)
		// System.out.println(sites.toString());
		//

		ComputingElement CE = new ComputingElement();
		CE.start();

		System.out.println("Done");

	}

}
