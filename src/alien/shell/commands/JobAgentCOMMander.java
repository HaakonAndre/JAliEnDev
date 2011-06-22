package alien.shell.commands;

import alien.user.AliEnPrincipal;

/**
 * @author ron
 *
 */
public class JobAgentCOMMander extends JAliEnCOMMander{
	
	/**
	 * Get the user
	 * 
	 * @return user
	 */
	public AliEnPrincipal getUser() {
		return user;
	}
	
	/**
	 * get the user's name
	 * 
	 * @return user name
	 */
	public String getUsername() {
		return user.getName();
	}

	/**
	 * Get the site
	 * 
	 * @return site
	 */
	public String getSite() {
		return site;
	}
	
}
