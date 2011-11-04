package alien.shell;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;

import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;
import alien.test.utils.Functions;

/**
 * @author ron
 * @since October 11, 2011
 */
public class ShellEditor {

	/**
	 * location of the vi binary
	 */
	private final static String cVI = Functions.which("vi");
	
	/**
	 * location of the vim binary
	 */
	private final static String cVIM = Functions.which("vim");
	
	/**
	 * location of the emacs binary
	 */
	private final static String cEmacs = Functions.which("emacs");
	
	/**
	 * location of the a user set editor binary
	 */
	private static String userSetEditor = "";
	
	/**
	 * location of current editor binary
	 */
	private static String editor = cVIM;
	
	
	protected static void editFile(String localFile){

		ArrayList<String> command  = new ArrayList<String>(2);
		command.add(editor);
		command.add(localFile);
		final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);
		
		System.out.println("Will start: " + command);

		try{
		
		//pBuilder.returnOutputOnExit(true);
		
		// pBuilder.directory();

		//pBuilder.timeout(10, TimeUnit.MINUTES);

		//pBuilder.redirectErrorStream(true);


			final ExitStatus exitStatus;


			exitStatus = pBuilder.start().waitFor();


		} catch (final InterruptedException ie) {
			System.err
					.println("Interrupted while waiting for the following command to finish : "
							+ command.toString());
		} catch (IOException e) {
			//ignore
		}
	}
	
	
	
	protected static void editOnConsole(String localFile){
		
		
		String command = "vim " + localFile;
        Process child;
		try {
			child = Runtime.getRuntime().exec(command);
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return;
		}
    
		
		
        // Read from an input stream
        InputStream in = child.getInputStream();
        OutputStream out = child.getOutputStream();
        InputStream err = child.getErrorStream();
     
		while (out != null) {

			BufferedReader processReader = new BufferedReader(
					new InputStreamReader(in));
			BufferedWriter processWriter = new BufferedWriter(
					new OutputStreamWriter(out));

			Console console = System.console();
			PrintWriter c_out = console.writer();
			Reader c_in = console.reader();
			
			BufferedReader cReader = new BufferedReader(
					c_in);

			String line;
			try {
				while ((line = processReader.readLine()) != null) {
					c_out.println(line);
				}
				while((line = cReader.readLine()) != null){
					processWriter.write(line);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		
		
//		if(console != null)
//		{
//			
//			String name = console.readLine("[Please Provide Your Name]: ");
//			char[] passdata = console.readPassword("[Please Input Your Password]: ");
//			Scanner scanner = new Scanner(console.reader());
//			int value = 0;
//			while(value != 99)
//			{
//				console.printf("Please input a value between 0 and 100.");
//				value = scanner.nextInt();				
//			}
//			
//
//		}
//		else
//		{
//			throw new RuntimeException("Can't run w/out a console!");
//		}
	}
	
	
	
	

}
