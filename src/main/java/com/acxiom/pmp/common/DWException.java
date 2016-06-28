/**
 * 
 */
package com.acxiom.pmp.common;

/**
 * @author kthota
 *
 */
public class DWException extends RuntimeException {


	/**
	 * @param message
	 */
	public DWException(String message) {
		super(message);
		 
	}

	/**
	 * @param cause
	 */
	public DWException(Throwable cause) {
		super(cause);
		 
	}

	/**
	 * @param message
	 * @param cause
	 */
	public DWException(String message, Throwable cause) {
		super(message, cause);
	
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public DWException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		 
	}

}
