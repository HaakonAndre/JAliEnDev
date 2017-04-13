package alien.servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import alien.catalogue.CatalogueUtils;
import alien.catalogue.IndexTableEntry;

/**
 * IndexTable lookups, based on cached indextable content
 *
 * @author costing
 * @since 2017-04-13
 */
public class IndexTableLookup extends HttpServlet {

	/**
	 *
	 */
	private static final long serialVersionUID = 1599249953957590702L;

	@Override
	protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
		final String lfn = req.getParameter("lfn");

		if (lfn != null && lfn.length() > 0) {
			final IndexTableEntry entry = CatalogueUtils.getClosestMatch(lfn);
			try (ServletOutputStream os = resp.getOutputStream()) {
				os.println("$VAR1 = [");
				os.println("  {");
				os.println("    'hostIndex' => '" + entry.hostIndex + "',");
				os.println("    'indexId' => '" + entry.indexId + "',");
				os.println("    'tableName' => '" + entry.tableName + "',");
				os.println("    'lfn' => '" + entry.lfn + "'");
				os.println("  }");
				os.println("];");
			}
		}
		else {
			final String refresh = req.getParameter("refresh");

			if (refresh != null && refresh.length() > 0)
				CatalogueUtils.invalidateIndexTableCache();
		}
	}
}
