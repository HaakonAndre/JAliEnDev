package alien.site;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import lia.util.Utils;
import alien.taskQueue.JDL;

class OutputEntry {
	private String name;
	private ArrayList<String> filesIncluded;
	private String options;
	private boolean isRootArchive;
	private ArrayList<String> ses;
	private ArrayList<String> exses;
	private HashMap<String, Integer> qos;
	
	public OutputEntry (String name, ArrayList<String> filesIncluded, String options){
		this.name 			= name;
		this.filesIncluded 	= filesIncluded;
		this.options 		= options;
		this.isRootArchive  = false;
		this.ses			= new ArrayList<>();
		this.exses			= new ArrayList<>();
		this.qos			= new HashMap<>();
		
		if(this.filesIncluded!=null){
			for (String f:this.filesIncluded){
				if(f.endsWith(".root")){
					this.isRootArchive = true;
					break;
				}
			}
		}
		
		// parse options
		if(this.options.length()>0){
			String[] opts = this.options.split(",");
			
			for (String o:opts){
				System.out.println("Parsing option: "+o);
				
				if(o.contains("=")){
					//e.g. disk=2
					String[] qosparts = o.split("=");
					qos.put(qosparts[0], Integer.parseInt(qosparts[1]));
				}else if(o.contains("!")){
					// de-prioritized se
					exses.add(o.substring(1));
				}else{
					// prioritized se
					ses.add(o);
				}
			}
		}

		System.out.println("QoS: "+qos.toString());
		System.out.println("SEs: "+ses.toString());
		System.out.println("ExSEs: "+exses.toString());
		
	}
	
	public String getName (){
		return this.name;
	}
	
	public ArrayList<String> getSEsPrioritized() {
		return ses;
	}
	
	public ArrayList<String> getSEsDeprioritized() {
		return exses;
	}
	
	public HashMap<String, Integer> getQoS() {
		return qos;
	}
	
 	public ArrayList<String> getFilesIncluded (){
		return this.filesIncluded;
	}
	
	public String getOptions (){
		return this.options;
	}

    public boolean isArchive() {
    	return this.filesIncluded != null && this.filesIncluded.size()>0;
    }

    public void createZip(String path){
    	if(path==null)
    		path=System.getProperty("user.dir");
    	if(!path.endsWith("/"))
    			path+="/";
    	
    	if(this.filesIncluded == null)
    		return;
    	
    	try {
    		// output file 
			ZipOutputStream out = new ZipOutputStream(new FileOutputStream(path+this.name));
			if(this.isRootArchive)
				out.setLevel(0);
			
			boolean hasPhysicalFiles = false;
			
			for (String file : this.filesIncluded){
				File f = new File(path+file);
				if (!f.exists() || !f.isFile() || !f.canRead() || f.length() <= 0){
					System.out.println("File "+file+" doesn't exist, cannot be read or has 0 size!");
					continue;
				}
				hasPhysicalFiles = true;
				
				// input file
				FileInputStream in = new FileInputStream(path+file);
				// name of the file inside the zip file 
		        out.putNextEntry(new ZipEntry(file)); 

		        byte[] b = new byte[1024];
		        int count;

		        while ((count = in.read(b)) > 0) {
		            out.write(b, 0, count);
		        }
		        in.close();
			}
			out.close();
			
			if(!hasPhysicalFiles)
				Files.delete(Paths.get(path+this.name));
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	return;
    } 
    
    public String toString(){
    	String toString =  "Name: "+this.name+" Options: "+this.options;
    	if(this.filesIncluded != null){
    		toString += this.filesIncluded.toString();
    	}
    	return toString;
    }
}

public class ParsedOutput {
	private ArrayList<OutputEntry> jobOutput;
	private JDL jdl;
	private int queueId;
	private String pwd;
	private String tag;
	
	public ParsedOutput(int queueId, JDL jdl){
		this.jobOutput	= new ArrayList<>();
		this.jdl 		= jdl;
		this.queueId 	= queueId;
		this.pwd		= "";
		this.tag		= "Output";
		parseOutput();
	}
	
	public ParsedOutput(int queueId, JDL jdl, String path){
		this.jobOutput	= new ArrayList<>();
		this.jdl 		= jdl;
		this.queueId 	= queueId;
		this.pwd 		= path+"/";
		this.tag		= "Output";
		parseOutput();
	}
	
	public ParsedOutput(int queueId, JDL jdl, String path, String tag){
		this.jobOutput	= new ArrayList<>();
		this.jdl 		= jdl;
		this.queueId 	= queueId;
		this.pwd 		= path+"/";
		this.tag 		= tag;
		parseOutput();
	}
	
	//TODELETE SYSOUTS
		
	public void parseOutput(){
		List<String> files = jdl.getOutputFiles(this.tag);
		
		if(files.size() == 0){
			// Create default archive
			files.add("jalien_defarchNOSPEC."+this.queueId+":stdout,stderr,resources");
		}
		System.out.println(files); //TODELETE
		
		for (final String line : files) {
			
			System.out.println("Line: "+line);
			
			String[] parts = line.split("@");
			
			System.out.println("Parts: "+parts[0]+" "+parts[1]);
						
			String options = parts.length>1 ? parts[1] : "";
			
			if (parts[0].contains(":")){
				// archive
				String[] archparts = parts[0].split(":");

				System.out.println("Archparts: "+archparts[0]+" "+archparts[1]);
				
				ArrayList<String> filesincluded = parsePatternFiles(archparts[1].split(","));	
				
				System.out.println("Adding archive: "+archparts[0]+" and opt: "+options);
				jobOutput.add(new OutputEntry(archparts[0], filesincluded, options));
			} else{
				// file(s)		
				System.out.println("Single file: "+parts[0]);
				ArrayList<String> filesincluded = parsePatternFiles(parts[0].split(","));
				for (String f:filesincluded){
					System.out.println("Adding single: ["+f+"] and opt: ["+options+"]");
					jobOutput.add(new OutputEntry(f, null, options));
				}
			}
		}
		
		System.out.println(jobOutput.toString()); //TODELETE
				
		return;
	}

	private ArrayList<String> parsePatternFiles(String[] files) {
		System.out.println("Files to parse patterns: "+Arrays.asList(files).toString());
		
		ArrayList<String> filesFound = new ArrayList<>();
		
		if(!pwd.equals("")){
			for (final String file:files){
				System.out.println("Going to parse: "+file);
				if (file.contains("*")){
					String[] parts = Utils.getOutput("ls "+pwd+file).split("\n");
					if(parts.length>0){
						for (String f:parts){
							f.trim();
							if(f.length()>0){
								String fname = new File(f).getName();								
								System.out.println("Adding file from ls: "+fname);
								filesFound.add(fname);
							}
						}
					}
				} else {
					filesFound.add(file);
				}
			}
		}
		
		System.out.println("Returned parsed array: "+filesFound.toString());
		
		return filesFound;
	}
	
	public ArrayList<OutputEntry> getEntries() {
		return this.jobOutput;
	}
	
}
