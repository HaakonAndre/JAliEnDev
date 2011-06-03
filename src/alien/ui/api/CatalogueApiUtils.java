package alien.ui.api;

import java.io.IOException;

import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.ui.Dispatcher;

/**
 * Get the GUID object for String
 * 
 * @author ron
 * @since Jun 03, 2011
 */
public class CatalogueApiUtils {

	public static LFN getLFN(String slfn) {
		return getLFN(slfn, false);
	}

	public static LFN getLFN(String slfn, boolean evenIfDoesNotExist) {

		try {
			LFNfromString rlfn = (LFNfromString) Dispatcher.execute(new LFNfromString(slfn, evenIfDoesNotExist));

			return rlfn.getLFN();
		} catch (IOException e) {
			System.out.println("Could not get LFN: " + slfn);
			e.printStackTrace();
		}
		return null;

	}
	
	public static GUID getGUID(String sguid) {
		return getGUID(sguid, false);
	}

	public static GUID getGUID(String sguid, boolean evenIfDoesNotExist) {

		try {
			GUIDfromString rguid = (GUIDfromString) Dispatcher.execute(new GUIDfromString(sguid, evenIfDoesNotExist));
			
		
			return rguid.getGUID();
		} catch (IOException e) {
			System.out.println("Could not get GUID: " + sguid);
		}
		return null;

	}
	
}
