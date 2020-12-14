package alien.io.protocols;

/**
 * @author Adrian Negru
 * @since Dec 14, 2020
 */
public enum SourceExceptionCode {
    /**
     * Server rejects the upload in case of an unforced attempt to upload an existing file
     */
    LOCAL_FILE_ALREADY_EXISTS,
    /**
     * Xrootd binaries could not be located on the filesystem, can't work without them
     */
    XRDCP_NOT_FOUND_IN_PATH,
    /**
     * Trying to get the file status with xrdfs failed to show the expected file details
     */
    XRDFS_CANNOT_CONFIRM_UPLOAD,
    /**
     * Java cannot launch other processes (out of handles / threads / other system resources?)
     */
    CANNOT_START_PROCESS,
    /**
     * The Java process was interrupted (signal received from outside) while waiting for the transfer process to return
     */
    INTERRUPTED_WHILE_WAITING_FOR_COMMAND,
    /**
     * Transfer took longer than allowed (with the 20KB/s + 60s overhead) so xrdcp was forcefully terminated 
     */
    XROOTD_TIMED_OUT,
    /**
     * Non-zero exit code from xrdcp, but no other details could be inferred from the command output
     */
    XROOTD_EXITED_WITH_CODE,
    /**
     * Download seemed to have finished correctly, but the local file doesn't match the expected catalogue size
     */
    LOCAL_FILE_SIZE_DIFFERENT,
    /**
     * For permissions or space reasons, the target local file could not be created
     */
    LOCAL_FILE_CANNOT_BE_CREATED,
    /**
     * The server couldn't locate (by broadcasting) any replica of the requested file 
     */
    NO_SERVERS_HAVE_THE_FILE,
    /**
     * The server returned an authoritative answer that the file is not present anywhere in its namespace
     */
    NO_SUCH_FILE_OR_DIRECTORY,
    /**
     * The file could be read from the storage and the size matches, but the content has a different md5 checksum than the expected one
     */
    MD5_CHECKSUMS_DIFFER,
    /**
     * The requested SE does not exist (any more)
     */
    SE_DOES_NOT_EXIST,
    /**
     * The GET method of this protocol is not implemented
     */
    GET_METHOD_NOT_IMPLEMENTED,
    /**
     * Any other error that doesn't fit anything from the above
     */
    INTERNAL_ERROR
}
