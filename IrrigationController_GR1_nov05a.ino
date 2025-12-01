#include "arduino_secrets.h"
#include <Arduino.h>
#include <SPI.h>
#include <Ethernet.h>
#include "OptaBlue.h"
using namespace Opta;

#include <ctime>                 // for time(), gmtime()
#include <ArduinoRS485.h>
#include <ArduinoModbus.h>

#include "thingProperties.h"     // Cloud variables & initProperties()
#include "Config.h"              // NUM_TOWERS, NUM_EXPANSIONS, PRESETS, ETH_MAC, REED_ACTIVE_HIGH, RELAY_ACTIVE_HIGH, LATE_TOLERANCE, CHANGE_INHIBIT_MS
#include "IOMap.h"               // REED_CH[], OUT_CH[]

// ======================================================================
// SMTP / EMAIL CONFIG
// ======================================================================

#ifndef SMTP_SERVER
#define SMTP_SERVER "mail.smtp2go.com"
#endif

#ifndef SMTP_PORT
#define SMTP_PORT 2525
#endif

#ifndef SMTP_USERNAME
#define SMTP_USERNAME "shlok@grohere.com"
#endif

#ifndef SMTP_PASSWORD
#define SMTP_PASSWORD "Strawberries2025$$"
#endif

#ifndef SMTP_USE_TLS
#define SMTP_USE_TLS 0   // MUST stay 0 with this simple client
#endif

#ifndef FAULT_EMAIL_FROM_NAME
#define FAULT_EMAIL_FROM_NAME "Tower Controller"
#endif

#ifndef FAULT_EMAIL_FROM_ADDR
#define FAULT_EMAIL_FROM_ADDR "shlok@grohere.com"
#endif

#ifndef FAULT_EMAIL_RECIPIENT_1_NAME
#define FAULT_EMAIL_RECIPIENT_1_NAME "Shlok"
#endif

#ifndef FAULT_EMAIL_RECIPIENT_1_ADDR
#define FAULT_EMAIL_RECIPIENT_1_ADDR "shlok@grohere.com"
#endif

#ifndef FAULT_EMAIL_RECIPIENT_2_NAME
#define FAULT_EMAIL_RECIPIENT_2_NAME ""
#endif

#ifndef FAULT_EMAIL_RECIPIENT_2_ADDR
#define FAULT_EMAIL_RECIPIENT_2_ADDR ""
#endif

#ifndef FAULT_EMAIL_RETRY_INTERVAL_MS
#define FAULT_EMAIL_RETRY_INTERVAL_MS 300000UL   // 5 minutes
#endif

// ======================================================================
// GENERIC HELPERS
// ======================================================================

inline bool timeReached(unsigned long t, unsigned long now) {
  return (long)(now - t) >= 0;
}

inline unsigned long uptimeSecNow() {
  return millis() / 1000UL;
}

inline unsigned long dayIndexNow() {
  return uptimeSecNow() / 86400UL;
}

String fmtUptime(unsigned long sec) {
  unsigned long d = sec / 86400UL; sec %= 86400UL;
  unsigned long h = sec / 3600UL;  sec %= 3600UL;
  unsigned long m = sec / 60UL;
  char buf[24];
  if (d > 0) snprintf(buf, sizeof(buf), "%lud %02lu:%02lu", d, h, m);
  else       snprintf(buf, sizeof(buf), "%02lu:%02lu", h, m);
  return String(buf);
}

static bool strHasValue(const char* s) {
  return s && s[0] != '\0';
}

// Base64 encoder for SMTP AUTH LOGIN
static String base64Encode(const char* input) {
  static const char alphabet[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  String out;
  if (!input) return out;
  size_t len = strlen(input);
  out.reserve(((len + 2) / 3) * 4);
  for (size_t i = 0; i < len; i += 3) {
    bool has2 = (i + 1) < len;
    bool has3 = (i + 2) < len;
    uint32_t chunk = ((uint32_t)(uint8_t)input[i]) << 16;
    if (has2) chunk |= ((uint32_t)(uint8_t)input[i + 1]) << 8;
    if (has3) chunk |= (uint32_t)(uint8_t)input[i + 2];

    char enc0 = alphabet[(chunk >> 18) & 0x3F];
    char enc1 = alphabet[(chunk >> 12) & 0x3F];
    char enc2 = has2 ? alphabet[(chunk >> 6) & 0x3F] : '=';
    char enc3 = has3 ? alphabet[chunk & 0x3F] : '=';

    out += enc0;
    out += enc1;
    out += enc2;
    out += enc3;
  }
  return out;
}

static bool smtpReadResponse(Client& client, int expectedCode, unsigned long timeoutMs = 8000UL) {
  unsigned long start = millis();
  String line;
  while ((millis() - start) < timeoutMs) {
    while (client.available()) {
      char ch = client.read();
      if (ch == '\r') continue;
      if (ch == '\n') {
        if (line.length() >= 3) {
          int code = line.substring(0, 3).toInt();
          bool more = (line.length() > 3 && line[3] == '-');
          if (!more) {
            return code == expectedCode;
          }
        }
        line = "";
      } else {
        line += ch;
      }
    }
  }
  return false;
}

static bool smtpCommand(Client& client, const String& cmd, int expectedCode) {
  client.print(cmd);
  client.print("\r\n");
  return smtpReadResponse(client, expectedCode);
}


// ======================================================================
// SECTION 1: TOWERS (Expansion 0)
// ======================================================================

// From tray-time analysis; sec*Hz
static const float TOWER_TRAY_TIME_K            = 9500.0f;
static const unsigned long MIN_TRAY_INTERVAL_MS = 60000UL;  // ignore double-beeps < 60s

struct Tower {
  Chan reed;
  Chan out;

  PinStatus     lastReed        = LOW;
  unsigned long lastEdgeMs      = 0;
  unsigned long prevEdgeMs      = 0;
  unsigned long lastIntervalMs  = 0;
  unsigned long lastChangeMs    = 0;

  float         speedHz         = PRESETS[0].speed;
  unsigned long interval_ms     = PRESETS[0].interval_ms;

  bool          contactorOn     = false;
  bool          fault           = false;
  unsigned long inhibitUntilMs  = 0;
  bool          bypassLine      = false;  // true = bypass ON (force coil on)

  bool          faultEmailSent        = false;
  unsigned long faultTriggeredMs      = 0;
  unsigned long faultExpectedIntervalMs = 0;
  bool          faultCommandedOn      = false;
  unsigned long lastFaultEmailAttempt = 0;
};

static Tower towers[NUM_TOWERS];
// Per-tower tray counts (resets daily)
unsigned long towerTrayCount[NUM_TOWERS];

// Per-tower irrigation valve-open counts
// (reset when irrigation is turned ON, and on daily reset)
unsigned int  towerIrrCount[NUM_TOWERS];

// Max valve activations per tower per irrigation run
// 16 trays + 1 safety pass
const uint8_t MAX_VALVE_OPENS_PER_TOWER = 17;


// ----------------------------------------------------------------------
// Tower fault email
// ----------------------------------------------------------------------

static bool sendTowerFaultEmail(const String& subject, const String& body) {
  if (!strHasValue(SMTP_SERVER)) {
    Serial.println(F("SMTP server not configured; skipping fault email."));
    return false;
  }
  if (SMTP_USE_TLS) {
    Serial.println(F("Configured SMTP server requires TLS which is not supported here."));
    return false;
  }

  const char* recipients[2];
  const char* names[2];
  size_t recipientCount = 0;
  if (strHasValue(FAULT_EMAIL_RECIPIENT_1_ADDR)) {
    recipients[recipientCount] = FAULT_EMAIL_RECIPIENT_1_ADDR;
    names[recipientCount]      = FAULT_EMAIL_RECIPIENT_1_NAME;
    recipientCount++;
  }
  if (strHasValue(FAULT_EMAIL_RECIPIENT_2_ADDR) && recipientCount < 2) {
    recipients[recipientCount] = FAULT_EMAIL_RECIPIENT_2_ADDR;
    names[recipientCount]      = FAULT_EMAIL_RECIPIENT_2_NAME;
    recipientCount++;
  }

  if (recipientCount == 0) {
    Serial.println(F("No fault email recipients configured."));
    return false;
  }

  EthernetClient client;
  Serial.print(F("Connecting to SMTP server "));
  Serial.print(SMTP_SERVER);
  Serial.print(F(":"));
  Serial.println(SMTP_PORT);
  if (!client.connect(SMTP_SERVER, SMTP_PORT)) {
    Serial.println(F("Unable to connect to SMTP server."));
    return false;
  }

  bool ok = smtpReadResponse(client, 220);
  if (ok) {
    if (!smtpCommand(client, F("EHLO tower-controller"), 250)) {
      ok = smtpCommand(client, F("HELO tower-controller"), 250);
    }
  }
  if (!ok) {
    Serial.println(F("SMTP greeting failed."));
    client.stop();
    return false;
  }

  if (strHasValue(SMTP_USERNAME)) {
    if (!smtpCommand(client, F("AUTH LOGIN"), 334)) {
      Serial.println(F("SMTP AUTH LOGIN rejected."));
      client.stop();
      return false;
    }
    String userEnc = base64Encode(SMTP_USERNAME);
    if (!smtpCommand(client, userEnc, 334)) {
      Serial.println(F("SMTP username rejected."));
      client.stop();
      return false;
    }
    String passEnc = base64Encode(SMTP_PASSWORD);
    if (!smtpCommand(client, passEnc, 235)) {
      Serial.println(F("SMTP password rejected."));
      client.stop();
      return false;
    }
  }

  String mailFrom = String("MAIL FROM:<") + FAULT_EMAIL_FROM_ADDR + ">";
  if (!smtpCommand(client, mailFrom, 250)) {
    Serial.println(F("SMTP MAIL FROM rejected."));
    client.stop();
    return false;
  }

  for (size_t i = 0; i < recipientCount; ++i) {
    String rcpt = String("RCPT TO:<") + recipients[i] + ">";
    if (!smtpCommand(client, rcpt, 250)) {
      Serial.print(F("SMTP RCPT TO rejected for "));
      Serial.println(recipients[i]);
      client.stop();
      return false;
    }
  }

  if (!smtpCommand(client, F("DATA"), 354)) {
    Serial.println(F("SMTP DATA command rejected."));
    client.stop();
    return false;
  }

  // Headers
  client.print(F("From: "));
  if (strHasValue(FAULT_EMAIL_FROM_NAME)) {
    client.print(FAULT_EMAIL_FROM_NAME);
    client.print(F(" <"));
  } else {
    client.print(F("<"));
  }
  client.print(FAULT_EMAIL_FROM_ADDR);
  client.print(F(">\r\n"));

  client.print(F("To: "));
  for (size_t i = 0; i < recipientCount; ++i) {
    if (i) client.print(F(", "));
    if (strHasValue(names[i])) {
      client.print(names[i]);
      client.print(F(" <"));
    } else {
      client.print(F("<"));
    }
    client.print(recipients[i]);
    client.print(F(">"));
  }
  client.print(F("\r\n"));

  client.print(F("Subject: "));
  client.print(subject);
  client.print(F("\r\n"));
  client.print(F("MIME-Version: 1.0\r\n"));
  client.print(F("Content-Type: text/plain; charset=utf-8\r\n"));
  client.print(F("Content-Transfer-Encoding: 8bit\r\n\r\n"));
  client.print(body);
  if (!body.endsWith("\r\n")) client.print(F("\r\n"));
  client.print(F(".\r\n"));

  if (!smtpReadResponse(client, 250)) {
    Serial.println(F("SMTP body send failed."));
    client.stop();
    return false;
  }

  smtpCommand(client, F("QUIT"), 221);
  client.stop();
  Serial.println(F("SMTP fault email dispatched."));
  return true;
}

static void notifyTowerFault(uint8_t idx, bool firstAttempt, unsigned long nowMs) {
  if (idx >= NUM_TOWERS) return;
  Tower &t = towers[idx];
  if (t.faultEmailSent) return;

  unsigned long sincePulse;
  if (t.lastEdgeMs == 0) {
    sincePulse = nowMs - t.faultTriggeredMs;
  } else {
    sincePulse = nowMs - t.lastEdgeMs;
  }
  unsigned long expected =
    t.faultExpectedIntervalMs ? t.faultExpectedIntervalMs : t.interval_ms;

  String subject = String("Tower ") + (idx + 1) + String(" fault: tray offline");
  if (!firstAttempt) subject += String(" (retry)");

  String body;
  body.reserve(256);
  body += String("Tower ") + (idx + 1) + String(" did not report activity as expected.\r\n");
  body += String("Expected interval: ") + String(((float)expected) / 1000.0f, 1) + String(" seconds\r\n");
  if (t.lastEdgeMs == 0) {
    body += String("Last pulse: no pulses recorded since startup\r\n");
  } else {
    body += String("Last pulse age: ") + String(((float)sincePulse) / 1000.0f, 1) + String(" seconds\r\n");
  }
  body += String("Commanded state before fault: ") + (t.faultCommandedOn ? String("ON") : String("OFF")) + String("\r\n");
  body += String("Line contactor bypass: ") + (t.bypassLine ? String("ENABLED") : String("DISABLED")) + String("\r\n");
  body += String("Preset speed (Hz): ") + String(t.speedHz, 1) + String("\r\n");
  body += String("Configured interval: ") + String(((float)t.interval_ms) / 1000.0f, 1) + String(" seconds\r\n\r\n");
  body += String("Please inspect the tray and acknowledge the fault once resolved.\r\n");

  bool sent = sendTowerFaultEmail(subject, body);
  t.lastFaultEmailAttempt = nowMs;
  if (sent) {
    t.faultEmailSent = true;
  }
}

// Tower helpers

static inline void setRelayChan(const Chan& c, bool on) {
  DigitalMechExpansion exp = OptaController.getExpansion(c.exp);
  exp.digitalWrite(
    c.ch,
    (RELAY_ACTIVE_HIGH ? (on ? HIGH : LOW) : (on ? LOW : HIGH))
  );
  // IMPORTANT: commit the change to the expansion outputs
  exp.updateDigitalOutputs();
}

void applyPresetToAll(uint8_t p) {
  if (p > 2) return;
  unsigned long now = millis();
  for (uint8_t i = 0; i < NUM_TOWERS; i++) {
    towers[i].speedHz        = PRESETS[p].speed;
    towers[i].interval_ms    = PRESETS[p].interval_ms;
    towers[i].inhibitUntilMs = now + CHANGE_INHIBIT_MS;
  }
}

void ackFault(uint8_t tIdx) {
  if (tIdx >= NUM_TOWERS) return;
  Tower &t = towers[tIdx];
  t.fault                   = false;
  t.faultEmailSent          = false;
  t.faultTriggeredMs        = 0;
  t.faultExpectedIntervalMs = 0;
  t.faultCommandedOn        = false;
  t.lastFaultEmailAttempt   = 0;
}

// Bypass semantics:
//  - bypassLine = true  → force relay ON (coil fed via O#), schedule ignores this tower
//  - bypassLine = false → coil controlled by schedule/fault logic via setContactor()
void applyBypass(uint8_t tIdx, bool bypass) {
  if (tIdx >= NUM_TOWERS) return;
  Tower &t = towers[tIdx];
  t.bypassLine = bypass;

  if (bypass) {
    // Force coil ON through the relay (bypass mode)
    t.fault        = false;
    t.contactorOn  = true;
    t.inhibitUntilMs = millis() + CHANGE_INHIBIT_MS;
    setRelayChan(t.out, true);
  } else {
    // Leaving bypass: clear timing baseline, schedule will take over
    t.contactorOn    = false;
    t.lastEdgeMs     = 0;
    t.prevEdgeMs     = 0;
    t.lastIntervalMs = 0;
    towerTrayCount[tIdx] = 0;     // start fresh counting for fault logic

    t.inhibitUntilMs = millis() + CHANGE_INHIBIT_MS;
    setRelayChan(t.out, false);
  }
}

void setContactor(uint8_t tIdx, bool on) {
  if (tIdx >= NUM_TOWERS) return;
  Tower &t = towers[tIdx];

  // In bypass mode we do NOT override the forced ON state here
  if (t.bypassLine) {
    return;
  }

  if (t.fault && on) {
    // require Ack before restart
    return;
  }

  t.contactorOn    = on;
  t.inhibitUntilMs = millis() + CHANGE_INHIBIT_MS;

  setRelayChan(t.out, on);
}

// Tray interval estimation from tower speed
unsigned long computeTowerIntervalMsFromHz(float hz) {
  if (hz <= 0.1f) return PRESETS[0].interval_ms;
  float sec = TOWER_TRAY_TIME_K / hz;     // sec = K / Hz
  if (sec < 60.0f)   sec = 60.0f;
  if (sec > 1200.0f) sec = 1200.0f;
  return (unsigned long)(sec * 1000.0f + 0.5f);
}

// Tower reed / fault logic with double-beep suppression

inline bool readReedActive(uint8_t i, PinStatus &raw) {
  DigitalMechExpansion exp = OptaController.getExpansion(REED_CH[i].exp);
  raw = exp.digitalRead(REED_CH[i].ch);
  return (REED_ACTIVE_HIGH ? (raw == HIGH) : (raw == LOW));
}

void readReedAndUpdate(uint8_t i, unsigned long now) {
  Tower &t = towers[i];
  PinStatus raw;
  bool active = readReedActive(i, raw);
  (void)active;

  // --- edge detection / tray counting ---
  if (raw != t.lastReed) {
    if (now - t.lastChangeMs >= REED_DEBOUNCE_MS) {
      t.lastChangeMs = now;

      // DEBUG: every raw change on this reed
      Serial.print(F("[REED_RAW] Tower "));
      Serial.print(i + 1);
      Serial.print(F(" raw="));
      Serial.print(raw == HIGH ? "HIGH" : "LOW");
      Serial.print(F(" lastReed="));
      Serial.print(t.lastReed == HIGH ? "HIGH" : "LOW");
      Serial.print(F(" contactorOn="));
      Serial.println(t.contactorOn ? "YES" : "NO");

      bool becameActive =
        (REED_ACTIVE_HIGH ?
         (raw == HIGH && t.lastReed == LOW) :
         (raw == LOW && t.lastReed == HIGH));

      // Only treat as tray if edge + contactor actually ON
      if (becameActive && t.contactorOn) {
        if (t.lastEdgeMs != 0) {
          unsigned long dt = now - t.lastEdgeMs;

          // ignore double-beeps closer than MIN_TRAY_INTERVAL_MS
          if (dt >= MIN_TRAY_INTERVAL_MS) {
            t.prevEdgeMs     = t.lastEdgeMs;
            t.lastIntervalMs = dt;
            t.lastEdgeMs     = now;

            towerTrayCount[i]++;

            Serial.print(F("[REED] Tower "));
            Serial.print(i + 1);
            Serial.print(F(" pulse, trayCount="));
            Serial.print(towerTrayCount[i]);
            Serial.print(F(", interval="));
            Serial.print(dt);
            Serial.println(F(" ms"));
          }
        } else {
          // first valid pulse since boot
          t.lastEdgeMs = now;
          towerTrayCount[i]++;

          Serial.print(F("[REED] Tower "));
          Serial.print(i + 1);
          Serial.print(F(" first pulse, trayCount="));
          Serial.println(towerTrayCount[i]);
        }
      }

      t.lastReed = raw;
    }
  }

  // --- fault detection ---
  // Do NOT generate faults while in bypass
  if (t.contactorOn && !t.fault && !t.bypassLine && now > t.inhibitUntilMs) {

    // don’t fault until at least one tray has been seen
    if (towerTrayCount[i] == 0) {
      return;
    }

    unsigned long lateThresh =
      (unsigned long)(t.interval_ms * (1.0f + LATE_TOLERANCE));
    unsigned long sincePulse =
      (t.lastEdgeMs == 0) ? (now - t.lastChangeMs) : (now - t.lastEdgeMs);

    if (sincePulse > lateThresh) {
      bool wasCommandedOn = t.contactorOn;

      Serial.print(F("[FAULT] Tower "));
      Serial.print(i + 1);
      Serial.print(F(" late. sincePulse="));
      Serial.print(sincePulse);
      Serial.print(F(" ms, lateThresh="));
      Serial.print(lateThresh);
      Serial.print(F(" ms, trayCount="));
      Serial.println(towerTrayCount[i]);

      t.fault                   = true;
      t.faultCommandedOn        = wasCommandedOn;
      t.faultExpectedIntervalMs = lateThresh;
      t.faultTriggeredMs        = now - sincePulse;
      t.faultEmailSent          = false;
      t.lastFaultEmailAttempt   = 0;
      t.contactorOn             = false;
      if (!t.bypassLine) setRelayChan(t.out, false);
      notifyTowerFault(i, true, now);
    }
  }
}

// ======================================================================
// SECTION 2: IRRIGATION (Expansion 1)
// ======================================================================

static const uint8_t  IRR_EXP_IDX      = 1;
static const uint8_t  NUM_CH           = 8;
static const unsigned long DEBOUNCE_MS = 10;
static unsigned long    PULSE_ON_MS    = 40000UL;
static const unsigned long IGNORE_MS   = 60000UL;
// ACTIVE_LEVEL = level when magnet PRESENT (inState == true)
static const PinStatus  ACTIVE_LEVEL   = HIGH;
static const bool       TRIGGER_ON_BOOT_HIGH = false;
static const unsigned long MIN_IRR_TRIGGER_MS = 30UL * 1000UL;  // ignore second trigger within 30 s

// State
PinStatus      lastRaw[NUM_CH];
PinStatus      lastStable[NUM_CH];
unsigned long  lastChangeMsIrr[NUM_CH];

unsigned long  ignoreUntilMs[NUM_CH];
unsigned long  offAtMs[NUM_CH];
uint8_t        outStateArr[NUM_CH];

unsigned long  trigCountArr[NUM_CH];
unsigned long  lastTrigMsArr[NUM_CH];
unsigned long  prevTrigMsArr[NUM_CH];
unsigned long  deltaSecArr[NUM_CH];

bool           firedThisAssert[NUM_CH];
// NEW: internal “real” enable flag
bool           irrigationEnabledInternal = true;

// Outage tracking for Cloud
bool          wasCloudConnected = false;
unsigned long disconnectStartMs = 0;
unsigned long lastUptimeBeforeDisconnectSec = 0;
unsigned long lastDayIndex = 0;

// Fire irrigation valve
inline void fireValve(DigitalMechExpansion& exp, uint8_t ch, unsigned long now) {
  // Map irrigation channel → tower index
  // Assumes: channel 0 → tower 0, channel 1 → tower 1, etc.
  if (ch < NUM_TOWERS) {
    // If this tower has already had its valve opened
    // MAX_VALVE_OPENS_PER_TOWER times in this run, do not irrigate again.
    if (towerIrrCount[ch] >= MAX_VALVE_OPENS_PER_TOWER) {
      return;  // irrigation for this tower is effectively "off"
    }
    towerIrrCount[ch]++;  // count this valve-on event
  }

  exp.digitalWrite(ch, HIGH);
  outStateArr[ch]     = 1;
  offAtMs[ch]         = now + PULSE_ON_MS;
  firedThisAssert[ch] = true;
  ignoreUntilMs[ch]   = now + IGNORE_MS;
  exp.updateDigitalOutputs();

  // If we just hit the max valve opens threshold, automatically
  // turn irrigation OFF so the dashboard toggle follows suit.
  if (ch < NUM_TOWERS &&
      towerIrrCount[ch] >= MAX_VALVE_OPENS_PER_TOWER) {
    Serial.print(F("Tower "));
    Serial.print(ch + 1);
    Serial.println(F(" reached max valve activations; disabling irrigation."));
    irrigationEnable          = false;
    irrigationEnabledInternal = false;
  }
}

// Publish irrigation + heartbeat to Cloud
void publishIrrigationToCloud(unsigned long nowMs) {
  static unsigned long nextPublish = 0;
  if (!timeReached(nextPublish, nowMs)) return;

  // Inputs (magnet present ⇒ true)
  inState0 = (lastStable[0] == ACTIVE_LEVEL);
  inState1 = (lastStable[1] == ACTIVE_LEVEL);
  inState2 = (lastStable[2] == ACTIVE_LEVEL);
  inState3 = (lastStable[3] == ACTIVE_LEVEL);
  inState4 = (lastStable[4] == ACTIVE_LEVEL);
  inState5 = (lastStable[5] == ACTIVE_LEVEL);
  inState6 = (lastStable[6] == ACTIVE_LEVEL);
  inState7 = (lastStable[7] == ACTIVE_LEVEL);

  // Outputs
  outState0 = (outStateArr[0] != 0);
  outState1 = (outStateArr[1] != 0);
  outState2 = (outStateArr[2] != 0);
  outState3 = (outStateArr[3] != 0);
  outState4 = (outStateArr[4] != 0);
  outState5 = (outStateArr[5] != 0);
  outState6 = (outStateArr[6] != 0);
  outState7 = (outStateArr[7] != 0);

  // Daily counts
  trigCount0 = (int)trigCountArr[0];
  trigCount1 = (int)trigCountArr[1];
  trigCount2 = (int)trigCountArr[2];
  trigCount3 = (int)trigCountArr[3];
  trigCount4 = (int)trigCountArr[4];
  trigCount5 = (int)trigCountArr[5];
  trigCount6 = (int)trigCountArr[6];
  trigCount7 = (int)trigCountArr[7];

  // Intervals (minutes)
  auto toMin = [](unsigned long s){ return (int)(s / 60UL); };
  deltaMin0 = toMin(deltaSecArr[0]);  deltaMin1 = toMin(deltaSecArr[1]);
  deltaMin2 = toMin(deltaSecArr[2]);  deltaMin3 = toMin(deltaSecArr[3]);
  deltaMin4 = toMin(deltaSecArr[4]);  deltaMin5 = toMin(deltaSecArr[5]);
  deltaMin6 = toMin(deltaSecArr[6]);  deltaMin7 = toMin(deltaSecArr[7]);

  unsigned long now = nowMs;
  lastTrigAgoMin0 = (lastTrigMsArr[0] == 0) ? -1 : (int)((now - lastTrigMsArr[0]) / 60000UL);
  lastTrigAgoMin1 = (lastTrigMsArr[1] == 0) ? -1 : (int)((now - lastTrigMsArr[1]) / 60000UL);
  lastTrigAgoMin2 = (lastTrigMsArr[2] == 0) ? -1 : (int)((now - lastTrigMsArr[2]) / 60000UL);
  lastTrigAgoMin3 = (lastTrigMsArr[3] == 0) ? -1 : (int)((now - lastTrigMsArr[3]) / 60000UL);
  lastTrigAgoMin4 = (lastTrigMsArr[4] == 0) ? -1 : (int)((now - lastTrigMsArr[4]) / 60000UL);
  lastTrigAgoMin5 = (lastTrigMsArr[5] == 0) ? -1 : (int)((now - lastTrigMsArr[5]) / 60000UL);
  lastTrigAgoMin6 = (lastTrigMsArr[6] == 0) ? -1 : (int)((now - lastTrigMsArr[6]) / 60000UL);
  lastTrigAgoMin7 = (lastTrigMsArr[7] == 0) ? -1 : (int)((now - lastTrigMsArr[7]) / 60000UL);

  // Heartbeat + IP
  uptimeSec = (int)(nowMs / 1000UL);
  uptimeStr = fmtUptime(uptimeSec);
  IPAddress ip = Ethernet.localIP();
  if (ip) {
    char buf[24];
    snprintf(buf, sizeof(buf), "%u.%u.%u.%u", ip[0], ip[1], ip[2], ip[3]);
    ipAddressStr = String(buf);
  } else {
    ipAddressStr = String("0.0.0.0");
  }

  nextPublish = nowMs + 10000UL;
}

// Cloud callbacks for irrigation
void onPulseSecondsChange() {
  if (pulseSeconds < 1)   pulseSeconds = 1;
  if (pulseSeconds > 600) pulseSeconds = 600;
  PULSE_ON_MS = (unsigned long)pulseSeconds * 1000UL;

  unsigned long now = millis();
  for (uint8_t ch = 0; ch < NUM_CH; ++ch)
    if (outStateArr[ch]) offAtMs[ch] = now + PULSE_ON_MS;
}

void onIrrigationEnableChange() {
  bool wasEnabled = irrigationEnabledInternal;

  // OPTION 1: dashboard ON = irrigation ON  (most intuitive)
  irrigationEnabledInternal = irrigationEnable;

  // When irrigation transitions from OFF → ON,
  // start a fresh count of valve opens per tower.
  if (!wasEnabled && irrigationEnabledInternal) {
    for (uint8_t i = 0; i < NUM_TOWERS; ++i) {
      towerIrrCount[i] = 0;
    }
  }

  // OPTION 2 (if you find it reversed in practice):
  // irrigationEnabledInternal = !irrigationEnable;
// handled in loop via assist path
}

// Stubs (not used, safe no-ops)
void onWindowStartSecChange() {}
void onWindowEndSecChange()   {}

// ======================================================================
// SECTION 3: TOWERS → CLOUD PUBLISH + BYPASS CALLBACKS
// ======================================================================

void publishTowersToCloud(unsigned long nowMs) {
  static unsigned long nextPublish = 0;
  if (!timeReached(nextPublish, nowMs)) return;

  unsigned long now = nowMs;

  auto edgeMin = [&](uint8_t i) -> int {
    if (towers[i].lastEdgeMs == 0) return -1;
    return (int)((now - towers[i].lastEdgeMs) / 60000UL);
  };
  auto intervalMin = [&](uint8_t i) -> int {
    if (towers[i].lastIntervalMs == 0) return -1;
    return (int)(towers[i].lastIntervalMs / 60000UL);
  };

  tower0Running         = towers[0].contactorOn;
  tower0Fault           = towers[0].fault;
  tower0LastEdgeMin     = edgeMin(0);
  tower0LastIntervalMin = intervalMin(0);
  tower0Bypass          = towers[0].bypassLine;

  tower1Running         = towers[1].contactorOn;
  tower1Fault           = towers[1].fault;
  tower1LastEdgeMin     = edgeMin(1);
  tower1LastIntervalMin = intervalMin(1);
  tower1Bypass          = towers[1].bypassLine;

  tower2Running         = towers[2].contactorOn;
  tower2Fault           = towers[2].fault;
  tower2LastEdgeMin     = edgeMin(2);
  tower2LastIntervalMin = intervalMin(2);
  tower2Bypass          = towers[2].bypassLine;

  tower3Running         = towers[3].contactorOn;
  tower3Fault           = towers[3].fault;
  tower3LastEdgeMin     = edgeMin(3);
  tower3LastIntervalMin = intervalMin(3);
  tower3Bypass          = towers[3].bypassLine;

  tower4Running         = towers[4].contactorOn;
  tower4Fault           = towers[4].fault;
  tower4LastEdgeMin     = edgeMin(4);
  tower4LastIntervalMin = intervalMin(4);
  tower4Bypass          = towers[4].bypassLine;

  tower5Running         = towers[5].contactorOn;
  tower5Fault           = towers[5].fault;
  tower5LastEdgeMin     = edgeMin(5);
  tower5LastIntervalMin = intervalMin(5);
  tower5Bypass          = towers[5].bypassLine;

  tower6Running         = towers[6].contactorOn;
  tower6Fault           = towers[6].fault;
  tower6LastEdgeMin     = edgeMin(6);
  tower6LastIntervalMin = intervalMin(6);
  tower6Bypass          = towers[6].bypassLine;

  tower7Running         = towers[7].contactorOn;
  tower7Fault           = towers[7].fault;
  tower7LastEdgeMin     = edgeMin(7);
  tower7LastIntervalMin = intervalMin(7);
  tower7Bypass          = towers[7].bypassLine;

  nextPublish = nowMs + 10000UL;
}

// Cloud callbacks for bypass toggles
void onTower0BypassChange() { applyBypass(0, tower0Bypass); }
void onTower1BypassChange() { applyBypass(1, tower1Bypass); }
void onTower2BypassChange() { applyBypass(2, tower2Bypass); }
void onTower3BypassChange() { applyBypass(3, tower3Bypass); }
void onTower4BypassChange() { applyBypass(4, tower4Bypass); }
void onTower5BypassChange() { applyBypass(5, tower5Bypass); }
void onTower6BypassChange() { applyBypass(6, tower6Bypass); }
void onTower7BypassChange() { applyBypass(7, tower7Bypass); }

// ======================================================================
// SECTION 4: VFD control over Modbus/RS485 (two MOVITRAC LTE-B+)
// ======================================================================

// Slave IDs for your SEW VFDs
const uint8_t VFD1_ID = 1;
const uint8_t VFD2_ID = 2;

// SEW MOVITRAC LTE-B process data, **0-based** for ArduinoModbus
const uint16_t REG_CMD_WORD     = 0;  // PO1 Control word (manual register 1)
const uint16_t REG_SPEED_REF    = 1;  // PO2 Setpoint speed (manual register 2)
const uint16_t REG_STATUS_WORD  = 5;  // PI1 Status word (manual register 6)
const uint16_t REG_ACTUAL_SPEED = 6;  // PI2 Actual speed (reg 7)
const uint16_t REG_ACTUAL_CURR  = 7;  // PI3 Actual current (reg 8)
const float VFD_MAX_FREQ_HZ = 53.3f;  // set this equal to P-01 on the drive

static const unsigned long VFD_POLL_INTERVAL_MS = 500;
static unsigned long nextVfdPollMs = 0;

// Hz ↔ drive units mapping (adjust scaling once confirmed)
inline uint16_t vfdEncodeSpeed(float hz) {
  if (hz < 0.0f)           hz = 0.0f;
  if (hz > VFD_MAX_FREQ_HZ) hz = VFD_MAX_FREQ_HZ;

  float perc = hz / VFD_MAX_FREQ_HZ;      // 0…1 of max Hz
  return (uint16_t)round(perc * 16384.0f);
}

inline float vfdDecodeSpeed(uint16_t raw) {
  float perc = (float)raw / 16384.0f;     // 0…1
  return perc * VFD_MAX_FREQ_HZ;
}


// Write command word and speed reference
static bool vfdWriteCommand(uint8_t slaveId, bool run, float speedHz) {
  // For MOVITRAC LTE-B:
  // 0 = stop, 6 = run forward via fieldbus (same as in Modbus Poll)
  uint16_t cmd = run ? 0x0006 : 0x0000;

  uint16_t speed = vfdEncodeSpeed(speedHz);

  // Best practice: write speed first, then the command word
  if (!ModbusRTUClient.holdingRegisterWrite(slaveId, REG_SPEED_REF, speed)) {
    return false;
  }
  if (!ModbusRTUClient.holdingRegisterWrite(slaveId, REG_CMD_WORD, cmd)) {
    return false;
  }

  return true;
}

// Read back status, speed, and alarm code
static bool vfdReadStatus(uint8_t slaveId,
                          int16_t &statusWord,
                          float   &speedHz,
                          int16_t &alarmCode) {

  // Read PI1 status (reg 6 → addr 5) and PI2 actual speed (reg 7 → addr 6)
  if (!ModbusRTUClient.requestFrom(slaveId, HOLDING_REGISTERS,
                                   REG_STATUS_WORD, 2)) {
    return false;
  }
  if (ModbusRTUClient.available() < 2) {
    return false;
  }

  uint16_t rawStatus = (uint16_t)ModbusRTUClient.read();
  uint16_t rawSpeed  = (uint16_t)ModbusRTUClient.read();

  statusWord = (int16_t)rawStatus;
  speedHz    = vfdDecodeSpeed(rawSpeed);

  // For now, no dedicated alarm register wired in – just return 0
  alarmCode  = 0;
  return true;
}


static void updateTowerIntervalFromVfdFeedback() {
  // Use VFD feedback to update tower interval_ms and speed
  // Separate feedback per drive
  bool have1 = (!vfd1CommFault && vfd1SpeedFbHz > 1.0f);
  bool have2 = (!vfd2CommFault && vfd2SpeedFbHz > 1.0f);

  if (!have1 && !have2) {
    // Neither drive has a valid speed yet → keep existing intervals
    return;
  }

  unsigned long int1 = 0;
  unsigned long int2 = 0;

  if (have1) {
    int1 = computeTowerIntervalMsFromHz(vfd1SpeedFbHz);
  }
  if (have2) {
    int2 = computeTowerIntervalMsFromHz(vfd2SpeedFbHz);
  }

  for (uint8_t i = 0; i < NUM_TOWERS; ++i) {
    if (i <= 3) {
      // Towers 1–4 (index 0–3) use VFD2
      if (have2) {
        towers[i].speedHz     = vfd2SpeedFbHz;
        towers[i].interval_ms = int2;
      }
    } else {
      // Towers 5–8 (index 4–7) use VFD1
      if (have1) {
        towers[i].speedHz     = vfd1SpeedFbHz;
        towers[i].interval_ms = int1;
      }
    }
  }
}

// Cloud callbacks
void onVfd1SpeedSetHzChange() {
  bool ok = vfdWriteCommand(VFD1_ID, vfd1RunCmd, vfd1SpeedSetHz);
  vfd1CommFault = !ok;
}

void onVfd2SpeedSetHzChange() {
  bool ok = vfdWriteCommand(VFD2_ID, vfd2RunCmd, vfd2SpeedSetHz);
  vfd2CommFault = !ok;
}

void onVfd1RunCmdChange() {
  bool ok = vfdWriteCommand(VFD1_ID, vfd1RunCmd, vfd1SpeedSetHz);
  vfd1CommFault = !ok;
}

void onVfd2RunCmdChange() {
  bool ok = vfdWriteCommand(VFD2_ID, vfd2RunCmd, vfd2SpeedSetHz);
  vfd2CommFault = !ok;
}

// If alarm code is changed manually from Cloud, we just keep it as telemetry
void onVfd2AlarmCodeChange() {
  // no-op; alarmCode is updated in pollVfdsIfDue
}

// Periodic polling
static void pollVfdsIfDue(unsigned long nowMs) {
  if ((long)(nowMs - nextVfdPollMs) < 0) return;

  // VFD #1
  {
    int16_t st = 0, al = 0;
    float   sp = 0.0f;
    bool ok = vfdReadStatus(VFD1_ID, st, sp, al);
    vfd1CommFault = !ok;
    if (ok) {
      vfd1StatusWord = (int)st;
      vfd1SpeedFbHz  = sp;
      vfd1AlarmCode  = (int)al;
    }
  }

  // VFD #2
  {
    int16_t st = 0, al = 0;
    float   sp = 0.0f;
    bool ok = vfdReadStatus(VFD2_ID, st, sp, al);
    vfd2CommFault = !ok;
    if (ok) {
      vfd2StatusWord = (int)st;
      vfd2SpeedFbHz  = sp;
      vfd2AlarmCode  = (int)al;
    }
  }

  // After updating feedback speeds, recompute expected tray interval
  updateTowerIntervalFromVfdFeedback();

  nextVfdPollMs = nowMs + VFD_POLL_INTERVAL_MS;
}

// Simple direct helpers (for debugging)
void readDriveStatus(uint8_t id) {
  uint16_t status = ModbusRTUClient.holdingRegisterRead(id, REG_STATUS_WORD);

  if (!ModbusRTUClient.lastError()) {
    Serial.print("Drive ");
    Serial.print(id);
    Serial.print(" Status Word: 0x");
    Serial.println(status, HEX);
  } else {
    Serial.print("Modbus Error reading status from VFD ");
    Serial.print(id);
    Serial.print(": ");
    Serial.println(ModbusRTUClient.lastError());
  }
}

void runDrive(uint8_t id, uint16_t speedValue) {
  bool okCmd = ModbusRTUClient.holdingRegisterWrite(id, REG_CMD_WORD, 0x0006); // run forward
  bool okSpd = ModbusRTUClient.holdingRegisterWrite(id, REG_SPEED_REF, speedValue);

  Serial.print("Drive ");
  Serial.print(id);
  Serial.print(" RUN: cmd=");
  Serial.print(okCmd);
  Serial.print(" spd=");
  Serial.println(okSpd);

  if (!okCmd || !okSpd) {
    Serial.print("Error code: ");
    Serial.println(ModbusRTUClient.lastError());
  }
}

void stopDrive(uint8_t id) {
  bool ok = ModbusRTUClient.holdingRegisterWrite(id, REG_CMD_WORD, 0x0000); // stop

  Serial.print("Drive ");
  Serial.print(id);
  Serial.print(" STOP: ");
  Serial.println(ok);

  if (!ok) {
    Serial.print("Error code: ");
    Serial.println(ModbusRTUClient.lastError());
  }
}

// Optional one-time test (call manually from setup() if desired)
void vfdConnectionTestOnce() {
  Serial.println("\n===== VFD CONNECTION TEST (VFD1 + VFD2) =====");

  // 1) Read status from both drives
  readDriveStatus(VFD1_ID);
  readDriveStatus(VFD2_ID);

  // 2) Start both drives at 25% speed
  Serial.println("Starting both drives @ 25% speed (4096/16384)...");
  runDrive(VFD1_ID, 4096);  // 25%
  runDrive(VFD2_ID, 4096);

  delay(3000);

  // 3) Stop both drives
  Serial.println("Stopping both drives...");
  stopDrive(VFD1_ID);
  stopDrive(VFD2_ID);

  Serial.println("===== VFD TEST COMPLETE =====");
}

// ======================================================================
// SECTION 5: SETUP & LOOP
// ======================================================================
static const float FIXED_TOWER_SPEED_HZ = 18.0f;


void setup() {
  Serial.begin(115200);
  OptaController.begin();
  // optional: simple log
  Serial.println("Opta controller + expansions initialised.");
  Serial.println();
  Serial.println("OPTA combined: Towers + Irrigation + VFDs");

  // Initial "null" values for Cloud variables
  ipAddressStr       = String("");
  lastOutageType     = String("none");
  uptimeStr          = String("");
  vfd1SpeedFbHz      = 0.0f;
  vfd1SpeedSetHz     = 0.0f;
  vfd2SpeedFbHz      = 0.0f;
  vfd2SpeedSetHz     = 0.0f;
  deltaMin0 = deltaMin1 = deltaMin2 = deltaMin3 =
  deltaMin4 = deltaMin5 = deltaMin6 = deltaMin7 = 0;
  lastOfflineDurationSec = 0;
  lastReconnectUptimeSec = 0;
  lastTrigAgoMin0 = lastTrigAgoMin1 = lastTrigAgoMin2 = lastTrigAgoMin3 =
  lastTrigAgoMin4 = lastTrigAgoMin5 = lastTrigAgoMin6 = lastTrigAgoMin7 = -1;
  networkLossCount = 0;
  powerLossCount   = 0;
  pulseSeconds     = 40;   // default 40 s
  irrigationEnable = true;

  tower0LastEdgeMin = tower0LastIntervalMin =
  tower1LastEdgeMin = tower1LastIntervalMin =
  tower2LastEdgeMin = tower2LastIntervalMin =
  tower3LastEdgeMin = tower3LastIntervalMin =
  tower4LastEdgeMin = tower4LastIntervalMin =
  tower5LastEdgeMin = tower5LastIntervalMin =
  tower6LastEdgeMin = tower6LastIntervalMin =
  tower7LastEdgeMin = tower7LastIntervalMin = -1;
  trigCount0 = trigCount1 = trigCount2 = trigCount3 =
  trigCount4 = trigCount5 = trigCount6 = trigCount7 = 0;
  uptimeSec = 0;
  vfd1AlarmCode = vfd1StatusWord = vfd2AlarmCode = vfd2StatusWord = 0;
  inState0 = inState1 = inState2 = inState3 =
  inState4 = inState5 = inState6 = inState7 = false;
  irrigationEnable = true;
    irrigationEnabledInternal = irrigationEnable;
  outState0 = outState1 = outState2 = outState3 =
  outState4 = outState5 = outState6 = outState7 = false;
  tower0Bypass = tower1Bypass = tower2Bypass = tower3Bypass =
  tower4Bypass = tower5Bypass = tower6Bypass = tower7Bypass = false;
  tower0Fault = tower1Fault = tower2Fault = tower3Fault =
  tower4Fault = tower5Fault = tower6Fault = tower7Fault = false;
  tower0Running = tower1Running = tower2Running = tower3Running =
  tower4Running = tower5Running = tower6Running = tower7Running = false;
  vfd1CommFault = vfd2CommFault = false;
  vfd1RunCmd = vfd2RunCmd = false;

  // Cloud / network
  initProperties();
  ArduinoCloud.begin(ArduinoIoTPreferredConnection);

  PULSE_ON_MS   = (unsigned long)pulseSeconds * 1000UL;
  lastDayIndex  = dayIndexNow();
  lastOutageType         = String("none");
  lastOfflineDurationSec = 0;
  powerLossCount         = 0;
  networkLossCount       = 0;
  lastReconnectUptimeSec = 0;

  // Opta + expansions
  OptaController.begin();

  // Towers init (exp0)
  unsigned long nowMs = millis();
  for (uint8_t i = 0; i < NUM_TOWERS; i++) {
    towers[i].reed           = REED_CH[i];
    towers[i].out            = OUT_CH[i];

    towers[i].lastReed       = LOW;
    towers[i].lastEdgeMs     = 0;
    towers[i].prevEdgeMs     = 0;
    towers[i].lastIntervalMs = 0;
    towers[i].lastChangeMs   = nowMs;

    towers[i].fault          = false;
    towers[i].faultEmailSent = false;
    towers[i].bypassLine     = false;     // default bypass ON (will be forced ON when user toggles)
    towers[i].contactorOn    = false;
    towers[i].inhibitUntilMs = nowMs + CHANGE_INHIBIT_MS;
  }

  // Reset tray counts at boot
  for (uint8_t i = 0; i < NUM_TOWERS; i++) {
    towerTrayCount[i] = 0;
    towerIrrCount[i]  = 0;
  }

  // Set default tower interval from fixed 18 Hz (overwritten later by VFD feedback)
  {
    unsigned long intMs = computeTowerIntervalMsFromHz(FIXED_TOWER_SPEED_HZ);
    for (uint8_t i = 0; i < NUM_TOWERS; ++i) {
      towers[i].speedHz     = FIXED_TOWER_SPEED_HZ;
      towers[i].interval_ms = intMs;
    }
  }

  // Prime inputs for all expansions
  for (uint8_t e = 0; e < NUM_EXPANSIONS; ++e) {
    DigitalMechExpansion ex = OptaController.getExpansion(e);
    ex.updateDigitalInputs();
  }

  for (uint8_t i = 0; i < NUM_TOWERS; i++) {
    DigitalMechExpansion ex = OptaController.getExpansion(REED_CH[i].exp);
    towers[i].lastReed = ex.digitalRead(REED_CH[i].ch);
  }

  // Ensure tower outputs off at boot
  for (uint8_t i = 0; i < NUM_TOWERS; i++) {
    setRelayChan(towers[i].out, false);
  }

  // Reflect default bypass to Cloud
  tower0Bypass = towers[0].bypassLine;
  tower1Bypass = towers[1].bypassLine;
  tower2Bypass = towers[2].bypassLine;
  tower3Bypass = towers[3].bypassLine;
  tower4Bypass = towers[4].bypassLine;
  tower5Bypass = towers[5].bypassLine;
  tower6Bypass = towers[6].bypassLine;
  tower7Bypass = towers[7].bypassLine;

  // Irrigation init (exp1)
  DigitalMechExpansion exp1 = OptaController.getExpansion(IRR_EXP_IDX);
  for (uint8_t ch = 0; ch < NUM_CH; ++ch) {
    exp1.digitalWrite(ch, LOW);
    outStateArr[ch]     = 0;
    offAtMs[ch]         = 0;
    ignoreUntilMs[ch]   = 0;
    trigCountArr[ch]    = 0;
    lastTrigMsArr[ch]   = 0;
    prevTrigMsArr[ch]   = 0;
    deltaSecArr[ch]     = 0;
    firedThisAssert[ch] = false;
  }
  exp1.updateDigitalOutputs();

  // Prime irrigation inputs
  exp1.updateDigitalInputs();
  unsigned long nowIrr = millis();
  for (uint8_t ch = 0; ch < NUM_CH; ++ch) {
    lastRaw[ch]         = exp1.digitalRead(ch);
    lastStable[ch]      = lastRaw[ch];
    lastChangeMsIrr[ch] = nowIrr;
    if (TRIGGER_ON_BOOT_HIGH &&
    lastStable[ch] == ACTIVE_LEVEL &&
    irrigationEnabledInternal) {
      trigCountArr[ch]  = 1;
      lastTrigMsArr[ch] = nowIrr;
      fireValve(exp1, ch, nowIrr);
    }
  }

  // RS485 / ModbusRTU for VFDs
  {
    const long VFD_BAUD = 19200;           // must match VFD
    const float bitDuration = 1.0f / (float)VFD_BAUD;
    const float wordLen     = 10.0f;       // 1 start + 8 data + parity + stop
    const unsigned long preDelay  = (unsigned long)(bitDuration * wordLen * 3.5f * 1e6f);
    const unsigned long postDelay = preDelay;

    RS485.begin(VFD_BAUD);
    RS485.setDelays(preDelay, postDelay);

    if (!ModbusRTUClient.begin(VFD_BAUD, SERIAL_8N1)) {
      Serial.println("Failed to start Modbus RTU Client");
      vfd1CommFault = true;
      vfd2CommFault = true;
    } else {
      Serial.println("Modbus RTU Client started");
      vfd1CommFault = false;
      vfd2CommFault = false;
    }

    vfd1SpeedSetHz = FIXED_TOWER_SPEED_HZ;
    vfd2SpeedSetHz = FIXED_TOWER_SPEED_HZ;
    vfd1RunCmd     = false;
    vfd2RunCmd     = false;
  }

  // Ethernet (for Cloud + SMTP)
  if (Ethernet.begin(ETH_MAC) == 0) {
    Serial.println("DHCP failed; using 192.168.1.222");
    IPAddress ip2(192,168,1,222), dns(192,168,1,1), gw(192,168,1,1), mask(255,255,255,0);
    Ethernet.begin(ETH_MAC, ip2, dns, gw, mask);
  }
  delay(200);
  IPAddress ip = Ethernet.localIP();
  Serial.print("IP: "); Serial.println(ip);

  // Optional: run one-time VFD connection test
  // vfdConnectionTestOnce();
}

void loop() {
  ArduinoCloud.update();

  unsigned long nowMs = millis();

  // Daily reset of irrigation counts and tower tray counts
  unsigned long di = dayIndexNow();
  if (di != lastDayIndex) {
    // irrigation
    for (uint8_t ch = 0; ch < NUM_CH; ++ch) {
      trigCountArr[ch] = 0;
    }
    // towers
    for (uint8_t i = 0; i < NUM_TOWERS; ++i) {
      towerTrayCount[i] = 0;
      towerIrrCount[i]  = 0;
    }
    lastDayIndex = di;
  }

  // Outage classification for Cloud
  bool isConn = ArduinoCloud.connected();
  if (wasCloudConnected && !isConn) {
    disconnectStartMs             = millis();
    lastUptimeBeforeDisconnectSec = uptimeSec;
  }
  if (!wasCloudConnected && isConn) {
    unsigned long nowSec = millis() / 1000UL;
    bool powerLoss = (nowSec + 5UL) < lastUptimeBeforeDisconnectSec;
    if (powerLoss) {
      powerLossCount++;
      lastOutageType         = String("power");
      lastOfflineDurationSec = 0;
    } else {
      networkLossCount++;
      lastOutageType         = String("network");
      lastOfflineDurationSec =
        (disconnectStartMs ? (int)((millis() - disconnectStartMs) / 1000UL) : 0);
    }
    lastReconnectUptimeSec = (int)nowSec;
    disconnectStartMs      = 0;
  }
  wasCloudConnected = isConn;

  // Refresh all expansion inputs
  for (uint8_t e = 0; e < NUM_EXPANSIONS; ++e) {
    DigitalMechExpansion ex = OptaController.getExpansion(e);
    ex.updateDigitalInputs();
  }

  // Towers (exp0): reed / fault logic
  for (uint8_t i = 0; i < NUM_TOWERS; i++) {
    readReedAndUpdate(i, nowMs);
  }

    // Tower run control – no time-of-day schedule, just fault + bypass
  {
    for (uint8_t i = 0; i < NUM_TOWERS; ++i) {
      // If bypass is ON, we don't touch this tower here
      if (towers[i].bypassLine) {
        continue;
      }

      // Desired state: run whenever the tower is NOT in fault
      bool desiredOn = !towers[i].fault;

      if (towers[i].contactorOn != desiredOn) {
        setContactor(i, desiredOn);
      }
    }
  }


  // Irrigation (exp1)
  DigitalMechExpansion exp1 = OptaController.getExpansion(IRR_EXP_IDX);

  // 1) Turn OFF irrigation outputs when time reached
  for (uint8_t ch = 0; ch < NUM_CH; ++ch) {
    if (outStateArr[ch] &&
        offAtMs[ch] &&
        timeReached(offAtMs[ch], nowMs)) {
      exp1.digitalWrite(ch, LOW);
      outStateArr[ch] = 0;
      offAtMs[ch]     = 0;
      exp1.updateDigitalOutputs();
    }
  }

  // 2) Edge-based counting and pulse
  for (uint8_t ch = 0; ch < NUM_CH; ++ch) {
    PinStatus raw = exp1.digitalRead(ch);
    if (raw != lastRaw[ch]) {
      lastRaw[ch]         = raw;
      lastChangeMsIrr[ch] = nowMs;
    }

    if ((nowMs - lastChangeMsIrr[ch]) >= DEBOUNCE_MS &&
        lastStable[ch] != raw) {

      PinStatus prev = lastStable[ch];
      lastStable[ch] = raw;

      // falling edge from ACTIVE_LEVEL → allow new assist later
      if (prev == ACTIVE_LEVEL &&
          lastStable[ch] != ACTIVE_LEVEL) {
        firedThisAssert[ch] = false;
      }

      // rising edge to ACTIVE_LEVEL → genuine magnet arrival
      if ((!ignoreUntilMs[ch] || timeReached(ignoreUntilMs[ch], nowMs)) &&
    (prev != ACTIVE_LEVEL && lastStable[ch] == ACTIVE_LEVEL)) {

  // NEW: reject triggers too close together
  if (lastTrigMsArr[ch] != 0 &&
      (nowMs - lastTrigMsArr[ch]) < MIN_IRR_TRIGGER_MS) {
    // treat as noise / duplicate, do not count
  } else {
    trigCountArr[ch]++;
    deltaSecArr[ch] =
      (prevTrigMsArr[ch] == 0) ? 0 : (nowMs - prevTrigMsArr[ch]) / 1000UL;
    prevTrigMsArr[ch] = nowMs;
    lastTrigMsArr[ch] = nowMs;

    if (irrigationEnabledInternal) fireValve(exp1, ch, nowMs);
else                           ignoreUntilMs[ch] = nowMs + IGNORE_MS;

  }
}

  }

  // 3) Assist path: only after at least one real edge
  if (irrigationEnabledInternal) {
    for (uint8_t ch = 0; ch < NUM_CH; ++ch) {
      if (lastStable[ch] == ACTIVE_LEVEL &&
          !outStateArr[ch] &&
          !firedThisAssert[ch] &&
          (!ignoreUntilMs[ch] || timeReached(ignoreUntilMs[ch], nowMs)) &&
          lastTrigMsArr[ch] != 0) {
        fireValve(exp1, ch, nowMs);
      }
    }
  }

  // VFD polling
  pollVfdsIfDue(nowMs);

  // Cloud publishing
  publishIrrigationToCloud(nowMs);
  publishTowersToCloud(nowMs);
}}