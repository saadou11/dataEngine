package com.socgen.daas.exceptions;

public class MissingKeysException extends Exception {
	
	// WORK IS NOT DONE YET
	
	static String MISSING_PROPERTIES_MSG = "Please check your application.properties file for mandatory properties";
	
	private static final long serialVersionUID = 157982962716077367L;

	public MissingKeysException() {
		super(MISSING_PROPERTIES_MSG);
	}
	
	public MissingKeysException(String message) {
		super(message);
	}

}
