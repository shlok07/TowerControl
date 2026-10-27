// ======================================================================
//  GrodanSensors.h  (v2, with dashboard status)
//
//  Reads Grodan GroSens rootzone data (WC, EC, temperature) from the
//  Grodan cloud API and copies it into Arduino Cloud variables.
//
//  Sensors:  RZ5290108 -> wc108, ec108, temp108
//            RZ5290119 -> wc119, ec119, temp119
//            RZ5290120 -> wc120, ec120, temp120
//  Status:   grodanStatus (String) shows what is happening, so you can
//            check it on the dashboard without a USB cable.
//
//  The MAIN sketch must contain, BEFORE #include "GrodanSensors.h":
//      const char GRODAN_USER[] = SECRET_GRODAN_USER;
//      const char GRODAN_PASS[] = SECRET_GRODAN_PASS;
//  (Arduino Cloud only creates Secret tab entries for names it finds
//   in the main sketch file.)
//
//  And at the end of loop():
//      GrodanSensors::update(nowMs, busy);
// ======================================================================

#pragma once

#include <EthernetSSLClient.h>
#include <ArduinoHttpClient.h>
#include <ArduinoJson.h>

namespace GrodanSensors {

// ---------- Settings ----------
const char HOST[] = "grosens-api.egro.grodan.com";
const int  PORT   = 443;

// All 3 sensors in one request (1 request toward the 1000/day limit).
const char DATA_PATH[] =
  "/data-api/v1/rootzone-data"
  "?sensorSerialNumbers=RZ5290108"
  "&sensorSerialNumbers=RZ5290119"
  "&sensorSerialNumbers=RZ5290120";

// Grodan sends water content as a fraction (0.7543 = 75.43 %).
const bool WC_AS_PERCENT = true;

const unsigned long FETCH_INTERVAL_MS     = 15UL * 60UL * 1000UL; // after success
const unsigned long RETRY_INTERVAL_MS     =  2UL * 60UL * 1000UL; // after a failure
const unsigned long FIRST_FETCH_DELAY_MS  = 60000UL;              // after boot
const unsigned long RESPONSE_TIMEOUT_MS   = 5000UL;
const unsigned long TOKEN_RENEW_MARGIN_MS = 30UL * 60UL * 1000UL;
const unsigned long BUSY_REPORT_MS        = 10UL * 60UL * 1000UL;

// ---------- Sensor to Cloud variable mapping ----------
struct Slot {
  const char* serial;
  float* wc;
  float* ec;
  float* temp;
};

Slot slots[] = {
  { "RZ5290108", &wc108, &ec108, &temp108 },
  { "RZ5290119", &wc119, &ec119, &temp119 },
  { "RZ5290120", &wc120, &ec120, &temp120 },
};
const int SLOT_COUNT = sizeof(slots) / sizeof(slots[0]);

// ---------- Internal state ----------
EthernetSSLClient sslClient;
HttpClient http(sslClient, HOST, PORT);

String accessToken  = "";
String refreshToken = "";
unsigned long tokenRenewAtMs = 0;

unsigned long nextFetchMs = FIRST_FETCH_DELAY_MS;
unsigned long busySinceMs = 0;
bool timeoutSet   = false;
bool bootReported = false;

// ---------- Status helpers ----------
inline String clockNow() {
  unsigned long t = ArduinoCloud.getLocalTime();
  if (t == 0) return String("");
  char buf[8];
  snprintf(buf, sizeof(buf), "%02lu:%02lu", (t / 3600UL) % 24UL, (t / 60UL) % 60UL);
  return String(buf);
}

inline void setStatus(const String& msg) {
  String stamp = clockNow();
  grodanStatus = stamp.length() ? (stamp + " " + msg) : msg;
  Serial.print(F("[GRODAN] "));
  Serial.println(grodanStatus);
}

inline String describeHttp(int code) {
  switch (code) {
    case -1: return "cannot connect to Grodan (network or SSL)";
    case -2: return "request error";
    case -3: return "Grodan did not answer in time";
    case -4: return "invalid reply from Grodan";
    case 400: return "HTTP 400 bad request";
    case 401: return "HTTP 401 unauthorized";
    case 403: return "HTTP 403 access denied";
    case 404: return "HTTP 404 not found";
    case 500: return "HTTP 500 Grodan server error";
    default:  return "HTTP " + String(code);
  }
}

// ---------- Login or refresh ----------
inline bool requestToken(bool useRefresh) {
  JsonDocument req;
  if (useRefresh) {
    req["refreshToken"] = refreshToken;
  } else {
    if (strlen(GRODAN_USER) == 0 || strlen(GRODAN_PASS) == 0) {
      setStatus("Login missing: fill SECRET_GRODAN_USER/PASS in Secret tab");
      return false;
    }
    req["username"] = GRODAN_USER;
    req["password"] = GRODAN_PASS;
  }

  String reqBody;
  serializeJson(req, reqBody);

  setStatus(useRefresh ? "Renewing token..." : "Logging in...");
  http.post("/data-api/v1/access-token", "application/json", reqBody);
  int status  = http.responseStatusCode();
  String body = http.responseBody();
  http.stop();

  if (status != 200 && status != 201) {
    if (status == 403 && !useRefresh)
      setStatus("Login failed: wrong e-Gro username or password");
    else
      setStatus("Login failed: " + describeHttp(status));
    Serial.println(body);
    return false;
  }

  JsonDocument doc;
  if (deserializeJson(doc, body)) {
    setStatus("Login failed: could not read reply");
    Serial.println(body);
    return false;
  }

  String newAccess = doc["accessToken"] | "";
  if (newAccess.length() == 0) {
    setStatus("Login failed: no token in reply");
    Serial.println(body);
    return false;
  }

  accessToken = newAccess;
  String newRefresh = doc["refreshToken"] | "";
  if (newRefresh.length() > 0) refreshToken = newRefresh;

  unsigned long expiresInMs = (unsigned long)(doc["expiresIn"] | 86400) * 1000UL;
  unsigned long lifeMs = (expiresInMs > TOKEN_RENEW_MARGIN_MS * 2)
                           ? expiresInMs - TOKEN_RENEW_MARGIN_MS
                           : expiresInMs / 2;
  tokenRenewAtMs = millis() + lifeMs;
  return true;
}

inline bool ensureToken(bool forceNew) {
  bool expired = (long)(millis() - tokenRenewAtMs) >= 0;
  if (accessToken.length() > 0 && !expired && !forceNew) return true;

  accessToken = "";
  if (refreshToken.length() > 0 && requestToken(true)) return true;
  refreshToken = "";
  return requestToken(false);
}

// ---------- GET with token ----------
inline int apiGet(const char* path, String& body) {
  String authHeader = "Bearer " + accessToken;

  http.beginRequest();
  http.get(path);
  http.sendHeader("Authorization", authHeader.c_str());
  http.sendHeader("Accept", "application/json");
  http.endRequest();

  int status = http.responseStatusCode();
  body = http.responseBody();
  http.stop();
  return status;
}

// ---------- Fetch and update. Returns true on success ----------
inline bool fetchNow() {
  if (!ensureToken(false)) return false;

  setStatus("Reading sensors...");
  String body;
  int status = apiGet(DATA_PATH, body);

  if (status == 401) {
    if (!ensureToken(true)) return false;
    status = apiGet(DATA_PATH, body);
  }

  if (status == 403) {
    setStatus("Data denied: sensors not assigned to this account");
    Serial.println(body);
    return false;
  }
  if (status != 200 && status != 206) {
    setStatus("Data failed: " + describeHttp(status));
    Serial.println(body);
    return false;
  }

  JsonDocument doc;
  if (deserializeJson(doc, body) || !doc.is<JsonArray>()) {
    setStatus("Data failed: unexpected reply format");
    Serial.println(body);
    return false;
  }

  int matched = 0;
  for (JsonVariant record : doc.as<JsonArray>()) {
    String serial = record["sensorSerialNumber"] | "";

    for (int i = 0; i < SLOT_COUNT; i++) {
      if (serial != slots[i].serial) continue;
      matched++;

      if (!record["waterContent"].isNull()) {
        float wc = record["waterContent"].as<float>();
        *slots[i].wc = WC_AS_PERCENT ? wc * 100.0f : wc;
      }
      if (!record["electricConductivity"].isNull())
        *slots[i].ec = record["electricConductivity"].as<float>();
      if (!record["rootzoneTemp"].isNull())
        *slots[i].temp = record["rootzoneTemp"].as<float>();

      Serial.print(F("[GRODAN] "));
      Serial.print(slots[i].serial);
      Serial.print(F(" WC="));  Serial.print(*slots[i].wc, 1);
      Serial.print(F(" EC="));  Serial.print(*slots[i].ec, 2);
      Serial.print(F(" T="));   Serial.println(*slots[i].temp, 1);
    }
  }

  if (matched == 0) {
    setStatus("Connected, but no matching sensors in reply");
    Serial.println(body);
    return false;
  }

  setStatus("OK (" + String(matched) + "/" + String(SLOT_COUNT) + " sensors)");
  return true;
}

// ---------- Call from the main loop() ----------
inline void update(unsigned long nowMs, bool busy) {
  if (!bootReported) {
    grodanStatus = "Started, first reading in 1 min";
    bootReported = true;
  }

  if ((long)(nowMs - nextFetchMs) < 0) return;   // not time yet

  if (!ArduinoCloud.connected()) return;          // no network yet

  if (busy) {
    if (busySinceMs == 0) busySinceMs = nowMs;
    else if (nowMs - busySinceMs > BUSY_REPORT_MS) {
      setStatus("Waiting: valve or VFD busy for 10+ min");
      busySinceMs = nowMs;
    }
    return;
  }
  busySinceMs = 0;

  if (!timeoutSet) {
    http.setHttpResponseTimeout(RESPONSE_TIMEOUT_MS);
    timeoutSet = true;
  }

  bool ok = fetchNow();
  nextFetchMs = millis() + (ok ? FETCH_INTERVAL_MS : RETRY_INTERVAL_MS);
}

} // namespace GrodanSensors