package alien.test.chapters;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.test.TestConfig;
import alien.test.setup.CreateLDAP;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
import alien.user.UserFactory;


/**
 * @author ron
 * @since October 09, 2011
 */
public class TestCentralUtils {
	
	
	private static int tno = 0;
	
	private static int cno = 0;
	
	
	
	/**
	 * @return status
	 */
	public static boolean runTestChapter(){
		
		tno = 3; 
		cno = 1;
		
		System.out.println("------------------------------------------------");	
		test("get /  ",possibleToGetCatalogueEntry("/"));
		System.out.println();
		test("get usr",possibleToGetCatalogueEntry(TestConfig.base_home_dir));
		System.out.println();
		test("get j/  ",possibleToGetCatalogueEntry(TestConfig.base_home_dir + TestConfig.testUser.substring(0,1)+"/"));
		System.out.println();
		test("get ~  ",possibleToGetCatalogueEntry(CreateLDAP.getUserHome("jalien")));
		
		System.out.println("==================");
		
		System.out.println();
		System.out.println("aliConfig is: "+System.getProperty("AliEnConfig"));
		
		String getIt = TestConfig.base_home_dir;
		//getIt = "/";
		System.out.println("getIt: " + getIt);
		
		LFN l1 = LFNUtils.getLFN(getIt);

		AliEnPrincipal user = UserFactory.getByUsername("admin");
		System.out.println("user: " + user.getName());
		
		String create = CreateLDAP.getUserHome("jalien")+"fuju";
		System.out.println("Create test: " + create);
		
		System.out.println("--------------------------------------");	
		possibleToAccessEntry(LFNUtils.getLFN("/"), user);
		System.out.println("--------------------------------------");	
		possibleToAccessEntry(LFNUtils.getLFN(CreateLDAP.getUserHome("jalien")), user);
		System.out.println("--------------------------------------");	
		possibleToAccessEntry(LFNUtils.getLFN(create,true), user);
		System.out.println("--------------------------------------");	

		
		//FileSystemUtils.createCatalogueDirectory(user,  create,true);
		l1 = LFNUtils.mkdir(user, create);
		
		//l1 = LFNUtils.getLFN(create);
		
		if(l1!=null){
			System.out.println("LFN: " + l1.getCanonicalName());
			System.out.println("LFN: " + l1.list());
		}
		else
			System.out.println(create + " is null.");
	
		System.out.println("----- TEST2 [DONE]: ooooooo -----");

	
		System.out.println("--------------------------------------");	
	
		return true;
	
	}
	
	
	private static boolean possibleToGetCatalogueEntry(String name){
		System.out.println("Get LFN test: [" + name + "]");
		LFN lfn = LFNUtils.getLFN(name);
		if(lfn!=null){
			System.out.println("LFN is: " + lfn.getCanonicalName());
			System.out.println("LFN parent: " + lfn.dir);
			
		}
		cno++;
		if(lfn!=null)
			return true;
		return false;
	}

	
	private static void possibleToAccessEntry(LFN lfn,
			AliEnPrincipal user) {

		if (AuthorizationChecker.canRead(lfn, user))
			System.out.println(user.getName() + " can read "
					+ lfn.getCanonicalName());
		else
			System.out.println(user.getName() + " canNOT read "
					+ lfn.getCanonicalName());
		if (AuthorizationChecker.canWrite(lfn, user))
			System.out.println(user.getName() + " can write "
					+ lfn.getCanonicalName());
		else
			System.out.println(user.getName() + " canNOT write "
					+ lfn.getCanonicalName());

	}
		
		
	private static void test(final String desc,final boolean res){
		System.out.print("----- TestCentralUtils "+cno+"/"+tno+" ["+desc+"]: ooooooo --------");
		if(res)
			System.out.println(" [ok]");
		else 
			System.out.println("!{NO}");
	}
	
	
	
}


