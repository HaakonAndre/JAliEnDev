package alien.test.chapters;

import alien.JSh;
import alien.catalogue.LFNUtils;
import alien.shell.BusyBox;
import alien.test.TestConfig;
import alien.test.setup.CreateLDAP;
import alien.test.utils.TestException;
import alien.test.utils.TestService;

/**
 * @author ron
 * @since October 25, 2011
 */
public class TestJShOverJBox {

	private static int tno = 0;

	private static int cno = 0;

	private static boolean finalStatus = true;

	private static BusyBox boombox = null;

	/**
	 * @throws Exception
	 */
	public static void runTestChapter() throws Exception {

		System.out.println();
		System.out
				.println("-----------------  Testing JSh over JBox -----------------");

		startJSh();
		test("JSh conntected ", jShConnected());

		if (boombox != null)
			System.out.println("Output of BOOMBOX ls: "
					+ boombox.callJAliEnGetString("ls"));
	}
	
	private static boolean jShConnected() throws Exception{
		if(boombox==null)
			throw new TestException("JSh's busybox is null.");
		String pwd = boombox.callJAliEnGetString("pwd");
		if(pwd==null)
			throw new TestException("JSh's busybox does not respond on pwd.");
		System.out.println("PWD IS: " + pwd);
		return (pwd == CreateLDAP.getUserHome(TestConfig.testUser));
		
		
	}
	
	

	private static void test(final String desc, final boolean res) {
		System.out.print("----- JSh over JBox Test " + cno + "/" + tno);
		if (res) {
			System.out.print(" [ok] ");
		} else {
			System.out.print(" {NO} !!!!!!");
			finalStatus = false;
		}
		System.out.println("    , which was: " + desc);
		cno++;
	}

	private static void startJSh() throws Exception {

		boombox = JSh.getBusyBox();
	}
}
