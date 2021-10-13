package at.windesign.importjson;

import org.json.JSONException;
import org.json.JSONTokener;
import org.json.JSONArray;
import org.json.JSONObject;

import snaq.util.jclap.CLAParser;
import snaq.util.jclap.OptionException;

import javax.net.ssl.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import java.sql.*;
import java.util.Date;


public class importJSON
{
	private static final int TYPE_LONG      = 1;
	private static final int TYPE_STRING    = 2;
	private static final int TYPE_DATE      = 3;
	private static final int TYPE_UNDEFINED = -1;

	private static final int OUTPUT_CSV   = 1;
	private static final int OUTPUT_SQL   = 2;
	private static final int OUTPUT_SQLDB = 3;

	public static void main(String[] args)
	{
		final CLAParser parser = new CLAParser();

		parser.addStringOption("i", "input", "input file path", false, false);
		parser.addStringOption("o", "output", "output file path (for download only)", false, false);
		parser.addStringOption("R", "request", "SELECT statement", false, false);
		parser.addStringOption("U", "url", "URL for Request", false, false);
		parser.addStringOption("T", "token", "Security Token for Dynastrace API", false, false);
		parser.addStringOption("B", "basename", "Base Name for downloaded file", false, false);
		parser.addStringOption("D", "date", "URL for request", false, false);
		parser.addIntegerOption("r", "range", "time range in minutes for download split (e.g. 30 for 30 minutes", false, false);
		parser.addBooleanOption("C", "outputcsv", "output format: CSV");
		parser.addBooleanOption("S", "outputsql", "output format: SQL");
		parser.addStringOption("s", "dbserver", "database server", false, false);
		parser.addStringOption("d", "db", "database", false, false);
		parser.addStringOption("t", "dbtable", "database table", false, false);
		parser.addStringOption("u", "dbuser", "database user", false, false);
		parser.addStringOption("p", "dbpass", "database password", false, false);
		parser.addStringOption("f", "timefields", "field name with time format", false, true);
		parser.addBooleanOption("h", "help", "Displays help information.", false);

		try
		{
			parser.parse(args);

			String       inputFile  = "";
			String       outputPath = "";
			String       request    = "";
			String       url        = "";
			String       token      = "";
			String       date       = "";
			int          range      = 0;
			String       baseName   = "";
			boolean      outputCSV  = false;
			boolean      outputSQL  = false;
			String       dbServer   = "";
			String       db         = "";
			String       dbTable    = "";
			String       dbUser     = "";
			String       dbPass     = "";
			List<String> timeFields = null;

			if(parser.getBooleanOptionValue("help"))
			{
				parser.printUsage(System.out, true, "importJSON", null);
				return;
			}

			inputFile  = parser.getStringOptionValue("input");
			outputPath = parser.getStringOptionValue("output");
			request    = parser.getStringOptionValue("request");
			url        = parser.getStringOptionValue("url");
			token      = parser.getStringOptionValue("token");
			baseName   = parser.getStringOptionValue("basename");
			date       = parser.getStringOptionValue("date");
			range      = parser.getIntegerOptionValue("range", 60);
			outputCSV  = parser.getBooleanOptionValue("outputcsv");
			outputSQL  = parser.getBooleanOptionValue("outputsql");
			dbServer   = parser.getStringOptionValue("dbserver");
			db         = parser.getStringOptionValue("db");
			dbTable    = parser.getStringOptionValue("dbtable");
			dbUser     = parser.getStringOptionValue("dbuser");
			dbPass     = parser.getStringOptionValue("dbpass");
			timeFields = parser.getStringOptionValues("timefields");

			if(baseName == null)
				baseName = "undefined";

			if(inputFile != null && (request != null || url != null))
			{
				System.out.println("Only one of -i (Input File) and -R/-U (Web) may be used.");
				parser.printUsage(System.out, true, "importJSON", null);
				return;
			}

			if(inputFile == null)
			{
				if(request == null || url == null)
				{
					System.out.println("For a web request both URL and REQUEST must be given.");
					parser.printUsage(System.out, true, "importJSON", null);
					return;
				}
			}

			if(outputPath == null || outputPath.isEmpty())
				outputPath = "./";

			if(outputPath.substring(outputPath.length() - 1, outputPath.length()).compareTo("/") != 0)
				outputPath += "/";

			int outputFormat = OUTPUT_CSV;

			if(outputCSV)
				outputFormat = OUTPUT_CSV;
			else if(outputSQL)
				outputFormat = OUTPUT_SQL;

			if(dbPass == null)
			{
				if(System.getenv().containsKey("MYSQL_PWD"))
					dbPass = System.getenv("MYSQL_PWD");
			}

			if(dbServer != null)
				outputFormat = OUTPUT_SQLDB;

			if(outputFormat == OUTPUT_SQL)
			{
				if(dbTable == null)
				{
					System.out.println("Required argument -t --dbtable is missing.");
					parser.printUsage(System.out, true, "importJSON", null);
					return;
				}
			}
			else if(outputFormat == OUTPUT_SQLDB)
			{
				if(dbServer == null)
				{
					System.out.println("Required argument -s --dbserver is missing.");
					parser.printUsage(System.out, true, "importJSON", null);
					return;
				}

				if(dbTable == null)
				{
					System.out.println("Required argument -t --dbtable is missing.");
					parser.printUsage(System.out, true, "importJSON", null);
					return;
				}

				if(dbUser == null)
				{
					System.out.println("Required argument -u --dbuser is missing.");
					parser.printUsage(System.out, true, "importJSON", null);
					return;
				}

				if(dbPass == null)
				{
					System.out.println("Required argument -p --dbpass is missing. Either set the argument or set the environment variable \"MYSQL_PWD\"");
					parser.printUsage(System.out, true, "importJSON", null);
					return;
				}
			}

			if(inputFile == null)
				downloadData(url, request, token, date, range, baseName, outputPath, outputFormat, timeFields, dbServer, db, dbTable, dbUser, dbPass);
			else
				processFile(inputFile, outputFormat, timeFields, dbServer, db, dbTable, dbUser, dbPass, "");
		}
		catch(OptionException e)
		{
			e.printStackTrace();
			parser.printUsage(System.out, true, "importJSON", null);
		}
		catch(ParseException e)
		{
			e.printStackTrace();
			parser.printUsage(System.out, true, "importJSON", null);
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
		}
	}

	private static boolean isTimeField(String header, List<String> timeFields)
	{
		for(int i = 0; i < timeFields.size(); i++)
		{
			if(header.equals(timeFields.get(i)))
				return true;
		}
		return false;
	}

	private static void processFile(String inputFile, int outputFormat, List<String> timeFields, String dbServer, String db, String dbTable, String dbUser, String dbPass, String date)
	{
		Connection con = null;

		if(outputFormat == OUTPUT_SQLDB)
		{
			System.out.print("Importing " + date + " ...");
			try
			{
				Class.forName("com.mysql.jdbc.Driver");
				con = DriverManager.getConnection("jdbc:mysql://" + dbServer + "/" + db + "?useSSL=false", dbUser, dbPass);
			}
			catch(ClassNotFoundException | SQLException e)
			{
				e.printStackTrace();
			}
		}

		List<String> header;
		header = new ArrayList<>();

		String insertIntoString = "INSERT INTO ";
		if(db != null)
			insertIntoString += db + ".";
		insertIntoString += dbTable + " (";

		try(FileReader reader = new FileReader(inputFile))
		{
			JSONTokener tokener = new JSONTokener(reader);
			JSONObject  obj     = new JSONObject(tokener);

			if(obj.has("columnNames"))
			{
				JSONArray columnNames = obj.getJSONArray("columnNames");

				for(int index = 0; index < columnNames.length(); index++)
				{
					if(index != 0)
					{
						if(outputFormat == OUTPUT_CSV)
							System.out.print(";");
						insertIntoString += ",";
					}

					String name = columnNames.getString(index);
					if(outputFormat == OUTPUT_CSV)
						System.out.print(name);
					header.add(name);
					insertIntoString += name.replace(".", "_");
				}

				if(outputFormat != OUTPUT_SQLDB)
					System.out.print("\n");
			}
			insertIntoString += ") VALUES (";

			String sqlStatement = "";
			if(obj.has("values"))
			{
				JSONArray values = (JSONArray) obj.get("values");

				for(int row = 0; row < values.length(); row++)
				{
					if(outputFormat == OUTPUT_SQL || outputFormat == OUTPUT_SQLDB)
						sqlStatement = insertIntoString;

					JSONArray rValue = values.getJSONArray(row);

					for(int col = 0; col < rValue.length(); col++)
					{
						String strValue = "";
						long   lValue   = -999999999;
						int    type     = TYPE_STRING;

						if(!rValue.isNull(col))
						{
							try
							{
								lValue = rValue.getLong(col);
								type   = TYPE_LONG;
							}
							catch(JSONException e)
							{
							}

							try
							{
								strValue = rValue.getString(col);
								strValue = strValue.replace("\"", "'");
								type     = TYPE_STRING;
							}
							catch(JSONException e)
							{
							}
						}
						else
							strValue = "";

						if(isTimeField(header.get(col), timeFields))
							type = TYPE_DATE;

						if(col != 0)
						{
							if(outputFormat == OUTPUT_CSV)
								System.out.print(";");
							if(outputFormat == OUTPUT_SQL || outputFormat == OUTPUT_SQLDB)
								sqlStatement += ",";
						}

						switch(type)
						{
							case TYPE_LONG:
								if(outputFormat == OUTPUT_CSV)
									System.out.print(lValue);
								if(outputFormat == OUTPUT_SQL || outputFormat == OUTPUT_SQLDB)
									sqlStatement += lValue;
								break;
							case TYPE_STRING:
								if(outputFormat == OUTPUT_CSV)
									System.out.print("\"" + strValue + "\"");
								if(outputFormat == OUTPUT_SQL || outputFormat == OUTPUT_SQLDB)
									sqlStatement += "\"" + strValue + "\"";
								break;
							case TYPE_DATE:
							{
								java.util.Date   d         = new java.util.Date(lValue);
								SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
								if(outputFormat == OUTPUT_CSV)
									System.out.print(formatter.format(d));
								if(outputFormat == OUTPUT_SQL || outputFormat == OUTPUT_SQLDB)
									sqlStatement += "\"" + formatter.format(d) + "\"";
							}
						}
					}

					sqlStatement += ");";

					if(outputFormat == OUTPUT_CSV)
						System.out.print("\n");
					else if(outputFormat == OUTPUT_SQLDB)
					{
						try
						{
							Statement stmt = con.createStatement();
							stmt.executeUpdate(sqlStatement);
						}
						catch(SQLException throwables)
						{
							throwables.printStackTrace();
						}
					}
					else if(outputFormat == OUTPUT_SQL)
					{
						System.out.print(sqlStatement);
						System.out.print("\n");
					}
				}
			}

			if(outputFormat == OUTPUT_SQLDB)
			{
				try
				{
					con.close();
					System.out.print("                                  success!\n");
				}
				catch(SQLException throwables)
				{
					throwables.printStackTrace();
				}
			}
		}
		catch(FileNotFoundException e)
		{
			e.printStackTrace();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}

	private static void downloadData(String url, String request, String token, String date, int range, String baseName, String outputPath, int outputFormat, List<String> timeFields, String dbServer, String db, String dbTable, String dbUser, String dbPass) throws java.text.ParseException, IOException
	{
		File outputDirectory = new File(outputPath);
		if(!outputDirectory.exists())
			outputDirectory.mkdirs();

		Date             d             = new SimpleDateFormat("yyyy-MM-dd").parse(date);
		SimpleDateFormat formatter     = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat formatterDate = new SimpleDateFormat("yyyy-MM-dd");
		Calendar         curDate       = Calendar.getInstance();
		curDate.setTime(d);

		for(; ; )
		{

			String fileName = null;
			try
			{
				fileName = downloadDate(url, request, token, curDate, range, baseName, outputPath, outputFormat, timeFields, dbServer, db, dbTable, dbUser, dbPass);
			}
			catch(NoSuchAlgorithmException e)
			{
				e.printStackTrace();
			}
			catch(KeyManagementException e)
			{
				e.printStackTrace();
			}
			processFile(fileName, outputFormat, timeFields, dbServer, db, dbTable, dbUser, dbPass, formatter.format(curDate.getTime()));

			curDate.add(Calendar.MINUTE, range);

			if(formatterDate.format(d).compareTo(formatterDate.format(curDate.getTime())) != 0)
				break;
		}
	}

	private static String downloadDate(String url, String request, String token, Calendar curDate, int range, String baseName, String outputPath, int outputFormat, List<String> timeFields, String dbServer, String db, String dbTable, String dbUser, String dbPass) throws IOException, NoSuchAlgorithmException, KeyManagementException
	{
		// Create a trust manager that does not validate certificate chains
		TrustManager[] trustAllCerts = new TrustManager[]
				{
						new X509TrustManager()
						{
							public java.security.cert.X509Certificate[] getAcceptedIssuers()
							{
								return null;
							}

							public void checkClientTrusted(X509Certificate[] certs, String authType)
							{
							}

							public void checkServerTrusted(X509Certificate[] certs, String authType)
							{
							}
						}
				};

		// Install the all-trusting trust manager
		SSLContext sc = SSLContext.getInstance("SSL");
		sc.init(null, trustAllCerts, new java.security.SecureRandom());
		HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

		// Create all-trusting host name verifier
		HostnameVerifier allHostsValid = new HostnameVerifier()
		{
			public boolean verify(String hostname, SSLSession session)
			{
				return true;
			}
		};

		// Install the all-trusting host verifier
		HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar         toDate    = Calendar.getInstance();
		toDate.setTime(curDate.getTime());
		toDate.add(Calendar.MINUTE, range);
		String start = formatter.format(curDate.getTime());
		String end   = formatter.format(toDate.getTime());

		if(outputFormat == OUTPUT_SQLDB)
			System.out.print("Downloading data for " + start + " - " + end + " ...");

		String SQL        = request.replace("%START%", start).replace("%END%", end);
		String strURL     = url + URLEncoder.encode(SQL, "UTF-8") + "&startTimestamp=1&addDeepLinkFields=false&explain=false";
		URL    urlRequest = new URL(strURL);
//		HttpURLConnection con        = (HttpURLConnection) urlRequest.openConnection();
		HttpsURLConnection con      = (HttpsURLConnection) urlRequest.openConnection();
		String             fileName = outputPath + baseName + " - " + formatter.format(curDate.getTime()).replace(":", "") + ".json";

		con.setRequestMethod("GET");
		con.setRequestProperty("accept", "application/json");
		con.setRequestProperty("Authorization", "Api-Token " + token);

		int            status  = con.getResponseCode();
		BufferedReader in      = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String         input;
		StringBuffer   content = new StringBuffer();

		while((input = in.readLine()) != null)
		{
			content.append(input);
		}
		in.close();

		BufferedWriter bwr = new BufferedWriter(new FileWriter(new File(fileName)));
		bwr.write(content.toString());
		bwr.flush();
		bwr.close();

		if(outputFormat == OUTPUT_SQLDB)
			System.out.print(" success!\n");

		return fileName;
	}
}
