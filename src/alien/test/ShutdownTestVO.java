package alien.test;

import alien.test.setup.CreateLDAP;

public class ShutdownTestVO {


	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		

		if (!TestBrain.findCommands()) {
			System.err.println("Necessary programs missing.");
			return;
		}

		
	CreateLDAP.stopLDAP();
	
	}
}
