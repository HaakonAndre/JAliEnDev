package utils.crawler;

/**
 * The crawling status for each PFN analysed
 *
 * @author anegru
 */
public enum CrawlingStatusCode {
    S_FILE_CHECKSUM_MATCH,
    S_FILE_CHECKSUM_MISMATCH,
    E_FILE_EMPTY,
    E_LFN_DOES_NOT_EXIST,
    E_PFN_NOT_READABLE,
    E_PFN_NOT_ONLINE,
    E_PFN_DOWNLOAD_FAILED,
    E_PFN_XRDSTAT_FAILED,
    E_FILE_MD5_IS_NULL,
    E_CATALOGUE_MD5_IS_BLANK,
    E_GUID_NOT_FOUND,
    E_UNEXPECTED_ERROR,
}