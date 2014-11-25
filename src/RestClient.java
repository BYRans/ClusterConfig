import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.collections.list.TreeList;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class RestClient {
	public final static String METHOD_GET = "GET";
	public final static String METHOD_PUT = "PUT";
	public final static String METHOD_DELETE = "DELETE";
	public final static String METHOD_POST = "POST";
	public static String URIprefix = "";
	public static String Authorization = "";
	public static Properties realKeyNameFile = new OrderedProperties();
	public static Properties configs = new OrderedProperties();

	static {

		try {
			configs.load(new FileInputStream(new File("src/config.properties")));//�������ļ�
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		URIprefix = getURIprefix();// �������ļ���ȡ����ǰ׺

		try {
			realKeyNameFile.load(new FileInputStream(new File( // ��ȡrealKeyName�ļ�
					"src/realKeyName.properties")));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// ��֤
		String userName = RestClient.configs.getProperty("userName");
		String password = RestClient.configs.getProperty("password");
		String message = "";
		if (userName == null)
			message += " userName";
		if (password == null)
			message += " password";
		if (!"".equals(message)) {
			System.out.println("Can`t find " + message
					+ " in the config file, please check the key`s spelling!");
			System.exit(0);
		}
		Authorization = userName + ":" + password;
	}

	// main
	public static void main(String args[]) {

		List<List<List>> secConfigs = getConfig();// ��ȡ�����ļ�,�������ļ����ηŵ�һ��list��secConfigs�д���Ǹ����ε�key��list��������cloudera_manager������

		if (secConfigs != null && secConfigs.size() > 0) {
			for (int i = 0; i < secConfigs.size(); i++) {
				String URIpostfix = getURIpostfix(secConfigs.get(i).get(0));// ƴ�Ӹ÷��������Ⱥ��URI��׺
				if ("getFailed".equals(URIpostfix)) {
					System.out.println(secConfigs.get(i).get(0).toString()
							+ "Ⱥ�鲻����");
					continue;
				}

				String URI = RestClient.URIprefix + URIpostfix;// URI

				String tempKeyRealName = "";
				String tempValue = "";
				List<String> tempKeyList = new ArrayList<String>();
				for (int j = secConfigs.get(i).get(0).size() - 1; j >= 1; j--) {
					String keyName = secConfigs.get(i).get(0).get(j).toString();
					String keyRealName = getRealKeyName(keyName);
					String value = secConfigs.get(i).get(1).get(j).toString();
					if ("hive_service_config_safety_valve".equals(keyRealName)
							|| "yarn_client_config_safety_valve"
									.equals(keyRealName)) {
						tempKeyRealName = keyRealName;
						tempValue += "<property>\\r\\n  <name>" + keyName
								+ "</name>\\r\\n  <value>" + value
								+ "</value>\\r\\n</property>\\n";// �������Ҫ׷��xml��key���ݴ浽temp��
						tempKeyList.add(keyName);
						continue;
					}

					// PUT
					String put = "{ \"items\": [{ \"name\": \"" + keyRealName
							+ "\", \"value\": \"" + value + "\" }] }";
					String respondMessage = rest(URI, put,
							RestClient.METHOD_PUT);

					// �����key�Ƿ����óɹ�
					System.out.println(respondMessage + " --> ["
							+ secConfigs.get(i).get(0).get(0).toString() + "]:"
							+ secConfigs.get(i).get(0).get(j).toString());
				}// �����for����
				
				if (!"".equals(tempKeyRealName)) {// ������temp�е���Ҫ׷��xml��key����
					String put = "{ \"items\": [{ \"name\": \""
							+ tempKeyRealName + "\", \"value\": \"" + tempValue
							+ "\" }] }";
					String respondMessage = rest(URI, put,
							RestClient.METHOD_PUT);
					for (String keyName : tempKeyList) {// ���׷��xml��key�Ƿ����óɹ�
						System.out.println(respondMessage + " --> ["
								+ secConfigs.get(i).get(0).get(0).toString()
								+ "]:" + keyName);
					}
				}
			}
		}
	}

	// ����Ϊ��λ��ȡ�ļ��������ڶ������еĸ�ʽ���ļ�
	public static List<List<List>> getConfig() {
		List<List<List>> secConfigs = new ArrayList<List<List>>();
		List<List> tempConfigList = null;
		List<String> tempKeyList = null;
		List<String> tempValueList = null;
		File file = new File("src/config.properties");
		BufferedReader reader = null;
		String[] temp;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int line = 1;
			while ((tempString = reader.readLine()) != null) {
				if ("".equals(tempString))
					continue;
				if (!tempString.contains("=")) {
					if (tempKeyList != null
							&& !"cloudera manager".equals(tempKeyList.get(0)
									.toString())) {
						tempConfigList.add(tempKeyList);
						tempConfigList.add(tempValueList);
						secConfigs.add(tempConfigList);
					}
					tempConfigList = new ArrayList();
					tempKeyList = new ArrayList();
					tempValueList = new ArrayList();
					tempKeyList.add(tempString);
					tempValueList.add("");
				} else {
					temp = tempString.split("=");
					tempKeyList.add(temp[0]);
					tempValueList.add(temp[1]);
				}
				line++;
			}
			tempConfigList.add(tempKeyList);
			tempConfigList.add(tempValueList);
			secConfigs.add(tempConfigList);
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
		return secConfigs;
	}

	public static String rest(String serviceUrl, String parameter,
			String restMethod) {
		StringBuffer response = new StringBuffer();
		int responseCode = 0;
		try {
			URL url = new URL(serviceUrl);
			HttpURLConnection con = (HttpURLConnection) url.openConnection();

			String encoding = new sun.misc.BASE64Encoder()
					.encode(RestClient.Authorization.getBytes());
			con.setRequestProperty("Authorization", "Basic " + encoding);

			// ����
			con.setRequestMethod(restMethod);
			// ������󷽷�ΪPUT,POST��DELETE����DoOutputΪ��
			if (RestClient.METHOD_PUT.equals(restMethod)
					|| RestClient.METHOD_POST.equals(restMethod)) {
				con.setDoOutput(true);
				con.setRequestProperty("Content-Type", "application/json");
				OutputStream os = con.getOutputStream();
				os.write(parameter.getBytes("UTF-8"));
				os.close();

				InputStream in = con.getInputStream();
				byte[] b = new byte[1024];
				int result = in.read(b);
				// while (result != -1) {
				// System.out.write(b, 0, result);
				// result = in.read(b);
				// }

				// ����put�����Ƿ�ɹ�
				responseCode = con.getResponseCode();
				if (responseCode == 200)
					return "success";
				else
					return "fail";
			} else { // ���󷽷�ΪGET��DELETEʱִ��
				InputStream in = con.getInputStream();
				byte[] b = new byte[4096];
				for (int n; (n = in.read(b)) != -1;) {
					response.append(new String(b, 0, n));
				}

				// ����get��������Ӧ����
				return response.toString();
			}
		} catch (Exception e) {
			System.out.println("Please check cloudera manager configuration and whether exist the key in realKeyName.properties.");
			return "fail";
		}
	}

	public static String getURIprefix() {
		String URIprefix = "";
		Properties pro = new OrderedProperties();
		try {
			pro.load(new FileInputStream(new File("src/config.properties")));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		String ip = pro.getProperty("ip");
		String port = pro.getProperty("port");
		String version = pro.getProperty("version");
		String clusterName = pro.getProperty("clusterName");

		if (ip != null && port != null && version != null
				&& clusterName != null)
			URIprefix = "http://" + ip + ":" + port + "/api/" + version
					+ "/clusters/" + clusterName + "/services/";
		else {
			String message = "";
			if (ip == null)
				message += "ip ";
			if (port == null)
				message += "port ";
			if (version == null)
				message += "version ";
			if (clusterName == null)
				message += "clusterName ";
			System.out.println("cloudera_manager config lack of " + message);
		}
		return URIprefix;
	}

	public static String getRoleConfigGroupName(String serviceName,
			String groupName) {
		String roleConfigGroupName = null;

		// GET
		String response = rest(RestClient.URIprefix + serviceName
				+ "/roleConfigGroups?view=full", null, RestClient.METHOD_GET);

		String json = response;
		try {
			JSONObject jsonobj = JSONObject.fromObject(json);// ���ַ���ת����json����
			JSONArray itemsArray = jsonobj.getJSONArray("items");// ��ȡ����
			// ��������
			if (itemsArray != null && itemsArray.size() > 0) {
				for (int i = 0; i < itemsArray.size(); i++) {
					JSONObject itemsObj = itemsArray.getJSONObject(i);
					if (groupName.equals(itemsObj.get("displayName")))
						roleConfigGroupName = itemsObj.getString("name");
				}
			}
		} catch (Exception e) {
			return "getFailed";
		}
		return roleConfigGroupName;
	}

	public static String getURIpostfix(List list) {
		String URIpostfix = null;
		String[] sectionName = list.get(0).toString().split("@");
		String serviceName = sectionName[0];
		String groupName = null;
		String roleConfigGroupName = null;
		if (sectionName.length == 1) {
			URIpostfix = serviceName + "/config";
		} else {
			groupName = sectionName[1];
			roleConfigGroupName = getRoleConfigGroupName(serviceName, groupName);
			URIpostfix = serviceName + "/roleConfigGroups/"
					+ roleConfigGroupName + "/config";
		}
		return URIpostfix;
	}

	public static String getRealKeyName(String showKey) {
		String realKeyName = RestClient.realKeyNameFile.getProperty(showKey);
		return realKeyName;
	}

}
