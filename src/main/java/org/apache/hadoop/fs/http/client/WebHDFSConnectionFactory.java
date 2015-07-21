package org.apache.hadoop.fs.http.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

/**
 * 
 * @author wesley
 * 
 */
public class WebHDFSConnectionFactory {

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	/** The default host to connect to */
	public static final String DEFAULT_HOST = "bi-hadoop-prod-2326.services.dal.bluemix.net";

	/** The default port */
	public static final int DEFAULT_PORT = 8443;

	/** The default username */
	public static final String DEFAULT_USERNAME = "biblumix";

	/** The default username */
	public static final String DEFAULT_PASSWORD = "a0~9fGkX@gqu";

	public static final String DEFAULT_PROTOCOL = "https://";

	public static enum AuthenticationType {
		KERBEROS, PSEUDO
	}

	private String host = DEFAULT_HOST;
	private int port = DEFAULT_PORT;
	private String username = DEFAULT_USERNAME;
	private String password = DEFAULT_PASSWORD;
	private String authenticationType = AuthenticationType.KERBEROS.name();
	private WebHDFSConnection webHDFSConnection;

	public WebHDFSConnectionFactory() {
	}

	public WebHDFSConnectionFactory(String host, int port, String username,
			String password, String authType) {
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.authenticationType = authType;
	}

	public WebHDFSConnection getConnection() {
		// TODO: use pool ...

		Assert.notNull(host, "Property <host> must not be null");
		Assert.notNull(port, "Property <port> must not be null");
		Assert.notNull(username, "Property <username> must not be null");
		Assert.notNull(authenticationType,
				"Property <authenticationType> must not be null");

		String httpfsUrl = DEFAULT_PROTOCOL + host + ":" + port + "/gateway/default/webhdfs/v1/";
		if (webHDFSConnection == null) {
			if (authenticationType.equalsIgnoreCase(AuthenticationType.KERBEROS
					.name())) {
				webHDFSConnection = new KerberosWebHDFSConnection(httpfsUrl,
						username, password);
			} else
			// if(authenticationType.equalsIgnoreCase(AuthenticationType.PSEUDO.name()))
			{
				webHDFSConnection = new PseudoWebHDFSConnection(httpfsUrl,
						username, password);
			}
		}
		return webHDFSConnection;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getAuthenticationType() {
		return authenticationType;
	}

	public void setAuthenticationType(String authenticationType) {
		this.authenticationType = authenticationType;
	}

}
