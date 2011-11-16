package alien;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashSet;

import lazyj.Utils;

import alien.catalogue.BookingTable;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.XrootDEnvelope;
import alien.io.protocols.Factory;
import alien.io.protocols.Protocol;
import alien.io.xrootd.envelopes.XrootDEnvelopeSigner;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;


/**
 * Testing stuff
 * 
 * @author costing
 *
 */
public class Testing {
	
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if (false){
			for (String file: Arrays.asList("fails", "works")){
				String s = Utils.readFile("/home/costing/workspace/playground/"+file+".txt");
				
				try {
					System.err.println(XrootDEnvelopeSigner.decrypt(s));
				}
				catch (GeneralSecurityException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			return;
		}
		
		AliEnPrincipal owner = UserFactory.getByUsername("aliprod");
		
		LFN targetLFN = LFNUtils.getLFN("/alice/cern.ch/user/a/aliprod/test/setest/ALICE::RAL::TAPE_test", true);
		
		SE se = SEUtils.getSE("ALICE::RAL::TAPE");
		
		File localFile = new File("/home/costing/pcalimonitor/bin/setesting/testfile");
		
		GUID guid = GUIDUtils.createGuid(localFile, owner);
		
		guid.lfnCache = new HashSet<LFN>();
		guid.lfnCache.add(targetLFN);
		
		PFN p = BookingTable.bookForWriting(targetLFN, guid, null, 0, se); 
		
		System.err.println(p);
		System.err.println(p.ticket.envelope.getEncryptedEnvelope());
		
		Protocol protocol = Factory.xrootd;
		
		String outcome = protocol.put(p, localFile);
		
		System.err.println(outcome);
	}
	
}
