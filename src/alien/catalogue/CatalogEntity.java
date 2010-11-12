/**
 * 
 */
package alien.catalogue;

/**
 * @author costing
 * @since Nov 11, 2010
 */
public interface CatalogEntity {

//	private static int IAm = 0;
//	private static int GUID = 1;
//	private static int PFN = 2;
		
	/**
	 * @return owner of the entity
	 */
	public String getOwner();
	
	/**
	 * @return group owner of the entity
	 */
	public String getGroup();
	
	/**
	 * @return permissions, in Unix style (such as 755)
	 */
	public String getPermissions();
	
	/**
	 * @return name of the entity
	 */
	public String getName();
	
	/**
	 * @return entry type (f,c,d,l)
	 */
	public char getType();
	
	/**
	 * @return type of CatalogEntity
	 */
	public char is();
	
}
