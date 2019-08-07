package com.exlibrisgroup.almaapis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.HttpsURLConnection;

import org.json.JSONArray;
import org.json.JSONObject;

import com.exlibrisgroup.bo.HttpResponse;

/**
 * For a detailed log use the following VM argument:
 * -Dlog4j.configuration=file:C:\\...\\log4j.properties
 */

class Task implements Runnable {

    String userId;

    public Task(String userId) {
        this.userId = userId;
    }

    public void run() {
        UpdatingUsers.updateUser(userId);
    }
}

public class UpdatingUsers {

    final private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(UpdatingUsers.class);
    private static final int HTTP_TOO_MANY_REQ = 429;
    private static final int MAX_NUM_OF_THREADS = 20;
    private static final int RETRY_TIMES = 3;

    private static final String BASE_URL = "https://api-eu.hosted.exlibrisgroup.com/almaws/v1/users/";
    private static final String API_KEY = "?apikey=l7xx.........";
    private static final String IDENTIFIER_TYPE = "Other";
    private static final String DEFAULT_EMAIL = "no@email.com";
    private static final String DEFAULT_PHONE = "1111111111";
    private static final String INPUT_FILE = "C:\\Temp\\UserUpdate\\input.txt";
    private static final double MAX_API_REQ_PER_DAY = 300000;

    public static void main(String[] args) {
        ExecutorService pool = Executors.newFixedThreadPool(MAX_NUM_OF_THREADS);
        // String userIds = "bdikazzz1 bdikaxxx1";
        // for (String userId : userIds.split(" ")) {
        for (String userId : readIdsFromFile()) {
            pool.execute(new Task(userId));
            // updateUser(userId); // sequential processing
        }
        logger.info("Done with main()");
    }

    public static void updateUser(String userId) {
        logger.info("Thread " + Thread.currentThread().getName() + " starting to handle user ID: " + userId + ".");

        logger.info("User ID: " + userId + " - calling GET");
        String url = BASE_URL + userId + API_KEY;

        HttpResponse outGetReq = sendHttpReqAndCheckThresholds(url, "GET", null);
        if (outGetReq.getResponseCode() != HttpsURLConnection.HTTP_OK) {
            logger.error("User not fetched correctly: " + userId + " - skip...\n");
            return;
        }

        JSONObject jsonObject = new JSONObject(outGetReq.getBody());

        try {
            fixMissingData(jsonObject);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getLocalizedMessage());
            return;
        }

        // PUT to change middle_name
        jsonObject.put("middle_name", "new middle name");
        HttpResponse outPutReq = sendHttpReqAndCheckThresholds(url, "PUT", jsonObject.toString());
        logger.debug("Output of PUT request: " + outPutReq.getBody());
        if (outPutReq.getResponseCode() != HttpsURLConnection.HTTP_OK) {
            logger.error("User not fetched correctly in PUT request: " + userId + " - skip...\n");
            return;
        }
        logger.info("Done. See: " + BASE_URL + userId + "\n");
    }

    private static List<String> readIdsFromFile() {
        List<String> userIds = new ArrayList<String>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(INPUT_FILE));
            String line;
            while ((line = br.readLine()) != null) {
                userIds.add(line);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("read " + userIds.size() + " lines from " + INPUT_FILE);
        return userIds;
    }

    /*
     * Users in the DB might be incomplete, to do wrong input upon the first
     * migration (or SIS load) and then PUT will fail. Here we'll fix a few
     * known issue.
     */
    private static void fixMissingData(JSONObject jsonObject) throws Exception {
        if (jsonObject.get("contact_info") == null) {
            throw new Exception("no contact_info");
        }
        JSONObject contactInfo = (JSONObject) jsonObject.get("contact_info");
        fixMissingEmail(contactInfo);
        fixMissingPhone(contactInfo);
    }

    private static void fixMissingEmail(JSONObject contactInfo) throws Exception {
        if (contactInfo.get("email") == null) {
            throw new Exception("no email");
        }
        JSONArray emailsInfo = (JSONArray) contactInfo.get("email");
        for (int i = 0; i < emailsInfo.length(); i++) {
            JSONObject oneEmailObj = emailsInfo.getJSONObject(i);
            if (!oneEmailObj.has("email_address")) {
                logger.info("missing email - adding");
                oneEmailObj.put("email_address", DEFAULT_EMAIL);
            }
            if (oneEmailObj.get("email_address").toString().isEmpty()) {
                logger.info("empty email - adding");
                oneEmailObj.remove("email_address");
                oneEmailObj.put("email_address", DEFAULT_EMAIL);
            }
        }
    }

    private static void fixMissingPhone(JSONObject contactInfo) throws Exception {
        if (contactInfo.get("phone") == null) {
            throw new Exception("no phone");
        }
        JSONArray phonesInfo = (JSONArray) contactInfo.get("phone");
        for (int i = 0; i < phonesInfo.length(); i++) {
            JSONObject onePhoneObj = phonesInfo.getJSONObject(i);
            if (!onePhoneObj.has("phone_number")) {
                logger.info("missing phone - adding");
                onePhoneObj.put("phone_number", DEFAULT_PHONE);
            }
            if (onePhoneObj.get("phone_number").toString().isEmpty()) {
                logger.info("empty phone - adding");
                onePhoneObj.remove("phone_number");
                onePhoneObj.put("phone_number", DEFAULT_PHONE);
            }
        }
    }

    private static String removeAdditionalId(JSONObject jsonObject) {
        logger.debug("Before changes:" + jsonObject);
        JSONArray identifiersArr = (JSONArray) jsonObject.get("user_identifier");
        if (identifiersArr != null) {
            for (int i = 0; i < identifiersArr.length(); i++) {
                JSONObject jsonIdentObj = identifiersArr.getJSONObject(i);
                if (jsonIdentObj.get("id_type") != null
                        && jsonIdentObj.getJSONObject("id_type").get("value").equals(IDENTIFIER_TYPE)) {
                    if (jsonIdentObj.get("value") != null) {
                        String additionalId = (String) jsonIdentObj.get("value");
                        identifiersArr.remove(i);
                        logger.debug("After changes:" + jsonObject);
                        return additionalId;
                    }
                }
            }
        }
        logger.debug("After changes:" + jsonObject);
        return null;
    }

    /**
     * If request returned 500 or 429 (too many per sec) we'll retry RETRY_TIMES
     * times
     */
    private static HttpResponse sendHttpReqAndCheckThresholds(String url, String method, String body) {
        HttpResponse httpResponse = null;
        for (int i = 0; i < RETRY_TIMES; i++) {
            httpResponse = sendHttpReq(url, method, body);
            if (httpResponse.getResponseCode() != HttpsURLConnection.HTTP_INTERNAL_ERROR
                    && httpResponse.getResponseCode() != HTTP_TOO_MANY_REQ) {
                // request was successful - no need to retry
                break;
            }
            logger.info("HTTP Code is : " + httpResponse.getResponseCode() + " Retrying...");
            try {
                // wait 1-5 sec. and try again
                Thread.sleep((new Random().nextInt(5) + 1) * 1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        checkThresholds(httpResponse.getHeaders());
        return httpResponse;
    }

    private static HttpResponse sendHttpReq(String url, String method, String body) {
        logger.info("Sending " + method + " request to URL : " + url.replaceAll("apikey=.*", "apikey=notOnLog"));
        try {
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod(method);
            con.setRequestProperty("Accept", "application/json");
            con.setRequestProperty("Content-Type", "application/json");

            if (body != null) {
                con.setDoOutput(true);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(con.getOutputStream(), "UTF-8"));
                bw.write(body);
                bw.flush();
                bw.close();
            }

            logger.info("Response Code : " + con.getResponseCode());

            BufferedReader in = null;
            if (con.getErrorStream() != null) {
                logger.error("reading con.getErrorStream()...");
                in = new BufferedReader(new InputStreamReader(con.getErrorStream()));
            } else {
                in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            }
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
                response.append(System.lineSeparator());
            }
            in.close();
            String out = response.toString().trim();

            // Log output of PUT only on error. Always log output of GET.
            if (!(method == "PUT" && con.getResponseCode() == HttpsURLConnection.HTTP_OK)) {
                logger.info("output: " + out);
            }
            con.disconnect();

            HttpResponse responseObj = new HttpResponse(out, con.getHeaderFields(), con.getResponseCode());

            return responseObj;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Warn when close to threshold (80%). Stop when really close (95%).
     */
    private static void checkThresholds(Map<String, List<String>> headersMap) {
        long apiRemaining = 0;
        try {
            apiRemaining = Long.parseLong(headersMap.get("X-Exl-Api-Remaining").get(0));
        } catch (Exception e) {
            return;
        }
        double remainingPercentage = 100 - (apiRemaining * 100D / MAX_API_REQ_PER_DAY);
        if (remainingPercentage >= 95D) {
            logger.error("Stopping - API Remaining - Not enough Api's for today ");
            Runtime.getRuntime().halt(0);
        }
        if (remainingPercentage >= 80D) {
            logger.warn("Warning - API Remaining - close to threshold (" + remainingPercentage + "%)");
        }
    }
}