package askul.business.quartz.dbsync;

import java.util.ArrayList;
import java.util.HashMap;

import askul.business.mail.TemplateMail;

public class DbSyncTracker {
	
	private TemplateMail postman = null;
	private static HashMap<String, ArrayList<String>> errors 
			= new HashMap<String, ArrayList<String>>();
	
	static {
		errors.put("main", new ArrayList<String>());
		errors.put("drop", new ArrayList<String>());
		errors.put("create", new ArrayList<String>());
		errors.put("insert", new ArrayList<String>());
	}
	
	public void sendMail(String[] mails) {
		postman.add("errors", errors);
		for (int i = 0; i < mails.length; i++) {
			postman.send(mails[i]);
		}
	}
	
	public void addError(String key, String message) {
		errors.get(key).add(message);
	}

	public TemplateMail getPostman() {
		return postman;
	}

	public void setPostman(TemplateMail mail) {
		this.postman = mail;
	}

	public HashMap<String, ArrayList<String>> getErrors() {
		return errors;
	}

}
