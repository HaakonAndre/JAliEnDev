package alien.taskQueue;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import alien.user.AliEnPrincipal;
import alien.user.UserFactory;

public class SplitTest {
	
		public static void main(final String[] args) throws IOException {
			String jdl = "";
			try(BufferedReader br = new BufferedReader(new FileReader("./src/main/java/alien/taskQueue/productionTest.txt"))) {
			    StringBuilder sb = new StringBuilder();
			    String line = br.readLine();

			    while (line != null) {
			        sb.append(line);
			        sb.append(System.lineSeparator());
			        line = br.readLine();
			    }
			    jdl = sb.toString();
			} catch (FileNotFoundException e) {
				System.out.println("No file found");
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			JDL j = new JDL(jdl);
			AliEnPrincipal user = UserFactory.getByUsername("admin");
			JobOptimizer jo = new JobOptimizer();
			
			List<JDL> results = jo.OptimizeJob(j, user, "haakon", 133334);
			
		}

}
