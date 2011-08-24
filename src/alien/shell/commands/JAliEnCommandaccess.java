package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.xml.sax.SAXException;

import com.sun.xml.internal.bind.v2.runtime.reflect.ListIterator;

import alien.api.catalogue.CatalogueApiUtils;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.perl.commands.AlienTime;
import alien.se.SE;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandaccess extends JAliEnBaseCommand {


	/**
	 * access request type: read,write,delete...
	 */
	private String request = "";


	/**
	 * access request LFN
	 */
	private LFN lfn;
	

	/**
	 * access request SEs (preferred/constraint SEs)
	 */
	private ArrayList<SE> ses;
	


	/**
	 * access request excluded SEs
	 */
	private ArrayList<SE> exxses;
	
	/**
	 * access request SE selector
	 */
	private int sesel = 0;
	

	/**
	 * access request site
	 */
	private String site = "";
	

	/**
	 * access request GUID
	 */
	private GUID guid;
	
	
	
	/**
	 * execute the access
	 */
	public void execute() {
		
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {
		//ignore
	}

	/**
	 * get cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * set command's silence trigger
	 */
	public void silent() {
		//ignore
	}

	
	

	/**
	 * serialize return values for gapi/root
	 * 
	 * @return serialized return
	 */
	public String deserializeForRoot() {
		//String ret = "";
//		if (directory != null) {
//			String col = RootPrintWriter.columnseparator;
//			String desc = RootPrintWriter.fielddescriptor;
//			String sep = RootPrintWriter.fieldseparator;
//
//			if (bL) {
//				for (LFN lfn : directory) {
//					ret += col;
//					ret += desc + "group" + sep + lfn.gowner;
//					ret += desc + "permissions" + sep + lfn.perm;
//					ret += desc + "date" + sep + lfn.ctime;
//					ret += desc + "name" + sep + lfn.lfn;
//					if(bF && (lfn.type == 'd'))
//						ret += "/";
//					ret += desc + "user" + sep + lfn.owner;
//					ret += desc + "path" + sep + lfn.dir;
//					ret += desc + "md5" + sep + lfn.md5;
//					ret += desc + "size" + sep + lfn.size;
//
//				}
//			} else if(bB){
//				for (LFN lfn : directory) {
//					ret += col;
//					ret += desc + "path" + sep + lfn.dir;
//					ret += desc + "guid" + sep + lfn.guid;
//				}
//			}else {
//				for (LFN lfn : directory) {
//					ret += col;
//					ret += desc + "name" + sep + lfn.lfn;
//					if(bF && (lfn.type == 'd'))
//						ret += "/";
//					ret += desc + "path" + sep + lfn.dir;
//				}
//			}
//
//			return ret;
//		} else
//			return super.deserializeForRoot();
		return "";

	}

	
	
	
//	
//	<stdoutindicator><columnseparator><fieldseparator>Aug 24 18:20:13  info	Authorize: STARTING envelope creation: -z read /alice/simulation/2008/v4-15-Release/Ideal/FMD/Align/Data/Run0_999999999_v1_s0.root 0 0 0 NIHAM 
//	<columnseparator>.<fieldseparator>Aug 24 18:20:13  info	Nothing from cache, going in: sesel=1,ses=
//	<columnseparator>.<fieldseparator>Aug 24 18:20:13  info	whereis said: ALICE::CERN::SE ALICE::CNAF::SE ALICE::FZK::SE ALICE::Legnaro::SE Alice::NIHAM::FILE ALICE::KFKI::SE
//	<columnseparator>.<fieldseparator>Aug 24 18:20:13  info	sql query: SELECT seName from (SELECT DISTINCT b.seName as seName, a.rank FROM SERanks a right JOIN SE b on (a.seNumber=b.seNumber and a.sitename=?) WHERE  (b.seExclusiveRead is NULL or b.seExclusiveRead = '' or b.seExclusiveRead  LIKE concat ('%,' , concat(? , ',%')) ) and  upper(b.seName)=upper(?) or upper(b.seName)=upper(?) or upper(b.seName)=upper(?) or upper(b.seName)=upper(?) or upper(b.seName)=upper(?) or upper(b.seName)=upper(?)  ORDER BY coalesce(a.rank,1000) ASC ) d;, values: CERN sschrein ALICE::CERN::SE ALICE::CNAF::SE ALICE::FZK::SE ALICE::Legnaro::SE Alice::NIHAM::FILE ALICE::KFKI::SE
//	<columnseparator>.<fieldseparator>Aug 24 18:20:13  info	We sorted inside ses<>0^sesel>1, outcome: sesel=1,ses=ALICE::CERN::SE,nSEs=6,sorted=ALICE::CERN::SE,ALICE::CNAF::SE,ALICE::KFKI::SE,ALICE::Legnaro::SE,Alice::NIHAM::FILE,ALICE::FZK::SE
//	<stderrindicator><columnseparator><fieldseparator><outputindicator><columnseparator><fielddescriptor>hashord<fieldseparator>turl-access-lfn-size-se-guid-md5-user-issuer-issued-expires-hashord<fielddescriptor>issuer<fieldseparator>Authen.v2-19.112<fielddescriptor>lfn<fieldseparator>/alice/simulation/2008/v4-15-Release/Ideal/FMD/Align/Data/Run0_999999999_v1_s0.root<fielddescriptor>pfn<fieldseparator>/03/54199/d04cfa3a-801c-11dd-ad0f-001e0b4e02fc<fielddescriptor>size<fieldseparator>4139<fielddescriptor>user<fieldseparator>sschrein<fielddescriptor>url<fieldseparator>root://pcaliense04.cern.ch:1095//03/54199/d04cfa3a-801c-11dd-ad0f-001e0b4e02fc<fielddescriptor>envelope<fieldseparator>-----BEGIN SEALED CIPHER-----
//	EBVdGI6ulcJR6fqDMqhUb5xf0bkC0WsJChx4Q7vIGcC8CZqMuP+bpXLXIBjLI+IEg0HSU+Tcnoqu
//	3Y+YnckQo4yC0EtysscaD9lROWcWQoJpzd0Mnem+X6hzGePnLzK1Ozp5L6ZQtFeGWxdcfVTxty0+
//	6E8RP49LYNwow8HX31c=
//	-----END SEALED CIPHER-----
//	-----BEGIN SEALED ENVELOPE-----
//	AAAAgHq+cg-wxP3RLZOCkYReUZAEPjCBzlSRkx-rJK01WO6pDrvUpuBWDyvlTJfYbJv-WHXZpB4x
//	jWvsuPXqhUw49fmDl3iSXUPLGMBhckfbheY75yRN1bFTlqYfpM8+mS3U73-5sJYn9e6GgFbXo089
//	8t+2HchICIrBIiRaDmaS1H0TwIAEWosqEkGj6dP+vq7b0P4kdrgNOL-Hkf9AGBftF9Wr2-J-eanQ
//	LrTCyPsqelcYPX6bK8tKyiLbgix2G5-N+XryJMY6p-oO6Fg5IG-RhUVFk6tOhf2g5uQori-PICnM
//	Pl4zDgJWHrxM+Vq6E0hX80BN646ubgKOq5qIP2+I5YyTp8BFuRxchIABzkVu8WqUEjG8o0DPqXUd
//	+4umnp9OYxDYkJJ6AS2Z2rw47WhZVGwDgKuPc6uDQu1lBkCU8HZm4GiM2lC+DEq9H+BenQ2AiqEy
//	flQGweFbzATLvs2ZZS2LOhqlJi6cMTh9FNSBIyzxKlTyWqQCBqq0jkbGZHzhsytVzLGnSe1NrIQM
//	p+vlrC6Hhv1eUv9SJH3nGs9mXJr8uiNE+9OgzXZzuzyf7fXOoAbECUASGM6ul2AjQ8QB01EkGJGU
//	1oJnvsbrEbdxg3oeZgSbjv9whjDy5op2HfyjMCSoCthkszcwvBMr2YrtEj5NWDNTnLafBbi1KwfF
//	z8gNxxqihkQJTZ0fZITkmZkvAYuf4O66m4nPQ2cDSWMY2JAJ6DAn3aMz6Xv2rTOgLcr4zGC2t-xB
//	CHV12PiVhtG6CZL0kqfzEoJF6nMsslwr-HNBH6LK6vdrrB0S9MnvY4vlxrRCbcTbHUGBK63F61aM
//	Bscn3fLpEdxzllRNDVR91I+6Ly-aBYIyhM7WBkPOLg5VfrOUEsxt6IukE4q8fzCMgl7k1epWMzZi
//	MZRlFk6rhI1f3ebtBwsyW31e99Pny86zsQD8DBxeklO1eCdCZrB9H3IPEyfrIsZUAGbjaDY0tsB+
//	apQGJVjyBOYslWkShmD1zI59FeoP5sfWi0Dydms5BcYssbjkajpjMFaF7PaNaZMvkzZnQXs-nVOs
//	XU-vMxotkTlgSvekyEw=
//	-----END SEALED ENVELOPE-----
//	<fielddescriptor>guid<fieldseparator>D04CFA3A-801C-11DD-AD0F-001E0B4E02FC<fielddescriptor>access<fieldseparator>read<fielddescriptor>nSEs<fieldseparator>6<fielddescriptor>se<fieldseparator>ALICE::CERN::SE<fielddescriptor>signature<fieldseparator>i0/bETKqxQdZg4is3NhWOUO1BQjvAFT+4a15P5J9A8L4htvYqOE/Z9yV2/O+VhmBQblYo4+0mg06ulEZk0MzAZABCnL9j0sRFtHDHd3/vbmtZrVX37+SoU4LbaXjJvnhuRG4847/rvwTCccvRmRus2E2X8hHF51Ds/Va0D3gTcM=<fielddescriptor>issued<fieldseparator>1314200567<fielddescriptor>md5<fieldseparator>0<fielddescriptor>expires<fieldseparator>1314286967<outputterminator><columnseparator><fielddescriptor>pwd<fieldseparator>/alice/cern.ch/user/s/sschrein/<streamend>
//	
	
	
	
	/**
	 * Constructor needed for the command factory in commander
	 * 
	 * @param commander
	 * @param out
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandaccess(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) {
		super(commander, out, alArguments);
		
		java.util.ListIterator<String> arg =  alArguments.listIterator();
		
		if(arg.hasNext()){
			request = arg.next();
			if(arg.hasNext()){
			lfn = CatalogueApiUtils.getLFN(arg.next());
			if(arg.hasNext()){
//				StringTokenizer 
//				arg.next()
//				CatalogueApiUtils.getSE(se);
//			}
//			
//				if(arg.hasNext()){
//					String lfn = 
//				} else{
//					out.printErrln("No LFN specified.")
//				}
			}
			}
			else if("write".equals(arg.next())) {
				out.printOutln("got write request");
			}
			
		}

	}

}
