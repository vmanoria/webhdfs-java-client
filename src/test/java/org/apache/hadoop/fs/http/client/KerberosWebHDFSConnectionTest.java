package org.apache.hadoop.fs.http.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KerberosWebHDFSConnectionTest {
	KerberosWebHDFSConnection conn = null;
	@Before
	public void setUp() throws Exception {
/*		String realm = System.getProperty("test.realm");
		String kdc = System.getProperty("test.kdc");
		String principal = System.getProperty("test.principal");
		String password = System.getProperty("test.password");
		String webhdfs = System.getProperty("test.webhdfs");
*/
		//String realm = System.getProperty("test.realm");
		//String kdc = System.getProperty("test.kdc");
		String principal = "biblumix";
		String password = "a0~9fGkX@gqu";
		String webhdfs = "https://bi-hadoop-prod-2326.services.dal.bluemix.net:8443/gateway/default/webhdfs/v1/";

		//System.setProperty("java.security.krb5.realm", realm);
		//System.setProperty("java.security.krb5.kdc", kdc);
		conn = new KerberosWebHDFSConnection(webhdfs, principal, password);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void getHomeDirectory() throws MalformedURLException, IOException, AuthenticationException {
		
		String json = conn.getHomeDirectory();
		System.out.println(json);
	}

	@Test
	public void listStatus() throws MalformedURLException, IOException, AuthenticationException {
		String path= "user/biblumix";
		String json = conn.listStatus(path);
		System.out.println(json);
	}

	@Test
	public void open() throws MalformedURLException, IOException, AuthenticationException {
		String path="user/biblumix/HealthCare/CloudantSyncCredentials.txt";
		FileOutputStream os = new  FileOutputStream(new File("/tmp/downloadfromhdfs.file"));
		String json = conn.open(path, os);
		System.out.println(json);
	}
	
	
	@Test
	public void create() throws MalformedURLException, IOException, AuthenticationException {
		FileInputStream is = new FileInputStream(new File("/tmp/downloadfromhdfs.file"));
		String path="/user/biblumix/hello.uploaded.txt";
		String json = conn.create(path, is);
		System.out.println(json);
	}

	@Test
	public void delete() throws MalformedURLException, IOException, AuthenticationException {
		String path="user/biblumix/hello.uploaded.txt"; 
		String json = conn.delete(path);
		System.out.println(json);
	}

}
