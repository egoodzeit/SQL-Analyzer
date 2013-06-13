package edu.rutgers.cs541;


/**
 * This is a simple class intended to 
 * be used as a return value that contains
 * more information than a single success/failure indication.
 * A text message can also be returned (such as
 * a reason for failure) and an exception can
 * also be returned.
 */
public class ReturnValue {

	// used to indicated whether an operation worked or not
	enum Code {
		SUCCESS,
		FAILURE
	}

	private Code mCode; 
	private String mMessage;
	private Exception mException;
	private ResultInfo mResult;
	
	//constructor with just a success or failure indication
	public ReturnValue(Code code) {
		mCode = code;
		mMessage = null;
		mException = null;
	}
	
	//constructor that includes a text message
	public ReturnValue(Code code, String message) {
		mCode = code;
		mMessage = message;
		mException = null;
	}
	
	//constructor that includes a text message and exception
	public ReturnValue(Code code, String message, Exception exception) {
		mCode = code;
		mMessage = message;
		mException = exception;
	}
	
	public ReturnValue(Code code, ResultInfo result)
	{
		mCode = code;
		mResult = result;
	}

	// getters & hassers
	public Code getCode() {
		return mCode;
	}
	public ResultInfo getResultInfo(){
		return mResult;
	}
	public boolean hasMessage() {
		return mMessage != null;
	}
	public String getMessage() {
		return mMessage;
	}
	public boolean hasException() {
		return mException != null;
	}
	public Exception getException() {
		return mException;
	}
}
