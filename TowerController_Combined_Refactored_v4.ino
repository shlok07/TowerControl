// ======================================================================
//  TowerController_Combined_Refactored.ino  v3
//
//  CHANGELOG vs v2:
//  [FIX-12] Extended VFD telemetry for SEW MOVITRAC LTE-B+:
//           - Motor current (PI3 reg 8, 0-based addr 7)
//           - Peak current (derived, resets daily)
//           - Acceleration ramp time (P-03, reg 131, 0-based 130) R/W
//           - Deceleration ramp time (P-04, reg 132, 0-based 131) Read
//           - Current limit (P-54, reg 182, 0-based 181) Read
//           Motor current polled at fast rate (500ms).
//           Parameters polled at slow rate (5s).
//           Accel time writable from cloud dashboard.
//
//  VFD REGISTER MAP (SEW MOVITRAC LTE-B+ manual §12.3.3):
//    Manual Reg | 0-Based | PD Word | Description
//    -----------+---------+---------+----------------------------------
//         1     |    0    |  PO1    | Control word        (R/W)
//         2     |    1    |  PO2    | Setpoint speed      (R/W)
//         3     |    2    |  PO3    | Ramp time override  (R/W)
//         6     |    5    |  PI1    | Status word         (R)
//         7     |    6    |  PI2    | Actual speed        (R)
//         8     |    7    |  PI3    | Actual current      (R)
//         9     |    8    |  PI4    | Motor torque        (R)
//       131     |  130    |  P-03   | Accel ramp time     (R/W)
//       132     |  131    |  P-04   | Decel ramp time     (R)
//       182     |  181    |  P-54   | Current limit       (R)
//
//  SCALING (SEW MOVITRAC LTE-B+):
//    Speed:   16384 (0x4000) = 100% of P-01 (max speed)
//    Current: 16384 (0x4000) = 100% of P-08 (rated motor current)
//    P-03/04: raw value × 0.01 = seconds  (e.g. 500 = 5.00 s)
//    P-54:    raw value × 0.1  = percent   (e.g. 1500 = 150.0%)
//
//  NOTE: Manual warns "Many bus masters address the first register as
//        register 0" — this code uses 0-based addresses throughout.
//
//  PRIOR CHANGELOG (v1→v2):
//  [FIX-1]  False faults after leaving bypass.
//  [FIX-2]  Proper debounce.
//  [FIX-3]  Separate fault tray counter from daily telemetry counter.
//  [FIX-4]  SMTP guarded – no attempt while irrigation valves active.
//  [FIX-5]  I2C expansion access with retry + error counting.
//  [FIX-6]  Hardware watchdog (IWDG).
//  [FIX-7]  Modbus inter-frame silent period between VFD1 and VFD2.
//  [FIX-8]  Irrigation stuck-sensor detection.
//  [FIX-9]  Reduced dynamic String allocations.
//  [FIX-10] Relay write-back verification placeholder.
//  [FIX-11] VFD ANTI-SLAM PROTECTION (Schneider FAQ FA173243).
//  [FIX-13] FALSE FAULT PREVENTION (data-driven, Jan 2026 CSV analysis):
//           - INTERVAL_HISTORY_SIZE: 2→4 (smoother rolling average)
//           - LEARNING_TRAYS: 3→5 (more data before arming)
//           - LATE_TOLERANCE: 0.30→0.50 (accommodates 30-35% CV)
//           - LEARNING_LATE_TOLERANCE: 0.80→1.00 (generous during learning)
//           - Speed-change grace period: when VFD speed changes >2Hz,
//             clear rolling history + extend inhibit 2× interval, so
//             4PM speed reduction doesn't trip Tower 4 on first slow tray.
//           - baseInterval now uses max(VFD interval, rolling avg) only
//             when rolling avg has ≥ INTERVAL_HISTORY_SIZE samples.
//  [FIX-14] INDEPENDENT IRRIGATION BATCHES + MULTI-CYCLE:
//           - Replaced single irrigationEnable with two per-batch toggles:
//             irrEnableBatch0 (towers 1-4), irrEnableBatch1 (towers 5-8).
//           - Added irrigationCycles (1-10): each cycle = 16 trays/tower.
//           - Removed sequential currentIrrBatch state machine.
//  [FIX-15] SLOT-BASED IRRIGATION PROGRESSION:
//           - 4 slots pair batch-0 with batch-1 towers:
//             slot 0: T1→T5, slot 1: T2→T6, slot 2: T3→T7, slot 3: T4→T8.
//           - Each slot runs ONE tower at a time; when it finishes (or
//             tower goes offline/maintenance), the slot advances to its
//             paired tower automatically. T2 down → T6 starts immediately.
//           - No waiting for all of T1-T4 to finish before T5-T8 begin.
//           - Periodic offline detection (5s) auto-advances stale slots.
//           - All slots complete → both batch toggles auto-disable.
// ======================================================================

#include "arduino_secrets.h"
#include <Arduino.h>
#include <SPI.h>
#include <Ethernet.h>
#include "OptaBlue.h"
using namespace Opta;

#include <ctime>
#include <ArduinoRS485.h>
#include <ArduinoModbus.h>

#include "thingProperties.h"
#include "Config.h"              // NUM_TOWERS, NUM_EXPANSIONS, PRESETS,
                                 // ETH_MAC, REED_ACTIVE_HIGH, RELAY_ACTIVE_HIGH,
                                 // LATE_TOLERANCE, CHANGE_INHIBIT_MS
#include "IOMap.h"               // REED_CH[], OUT_CH[]

// [FIX-6] Hardware watchdog.
#define USE_HW_WATCHDOG 0

#if USE_HW_WATCHDOG
#include <IWatchdog.h>
#endif

// ======================================================================
// SMTP / EMAIL CONFIG
// ======================================================================

#ifndef SMTP_SERVER
#define SMTP_SERVER SECRET_SMTP_SERVER
#endif
#ifndef SMTP_PORT
#define SMTP_PORT SECRET_SMTP_PORT
#endif
#ifndef SMTP_USERNAME
#define SMTP_USERNAME SECRET_SMTP_USERNAME
#endif
#ifndef SMTP_PASSWORD
#define SMTP_PASSWORD SECRET_SMTP_PASSWORD
#endif
#ifndef SMTP_USE_TLS
#define SMTP_USE_TLS 0
#endif
#ifndef FAULT_EMAIL_FROM_NAME
#define FAULT_EMAIL_FROM_NAME SECRET_FAULT_EMAIL_FROM_NAME
#endif
#ifndef FAULT_EMAIL_FROM_ADDR
#define FAULT_EMAIL_FROM_ADDR SECRET_FAULT_EMAIL_FROM_ADDR
#endif
#ifndef FAULT_EMAIL_RECIPIENT_1_NAME
#define FAULT_EMAIL_RECIPIENT_1_NAME SECRET_FAULT_EMAIL_RECIPIENT_1_NAME
#endif
#ifndef FAULT_EMAIL_RECIPIENT_1_ADDR
#define FAULT_EMAIL_RECIPIENT_1_ADDR SECRET_FAULT_EMAIL_RECIPIENT_1_ADDR
#endif
#ifndef FAULT_EMAIL_RETRY_INTERVAL_MS
#define FAULT_EMAIL_RETRY_INTERVAL_MS 300000UL
#endif

// ======================================================================
// TUNING CONSTANTS
// ======================================================================

// [FIX-13] Tuning constants — data-driven values from Jan 2026 analysis.
// Towers 4 & 7 have CV=30-35%, so 50% tolerance gives comfortable margin.
// 4-sample rolling average smooths out individual slow trays.
// 5 learning trays builds enough baseline before arming.
static const uint8_t  LEARNING_TRAYS            = 5;
static const uint8_t  INTERVAL_HISTORY_SIZE     = 4;
static const float    LEARNING_LATE_TOLERANCE   = 1.00f;

// [FIX-13] Speed-change grace: when VFD feedback changes >2 Hz between
// polls, we clear the rolling history for that group's towers and extend
// the inhibit period by 2× the new VFD interval. This prevents the 4PM
// speed reduction from tripping faults on the first slow tray.
static const float    SPEED_CHANGE_THRESHOLD_HZ = 2.0f;

#ifndef REED_DEBOUNCE_MS
#define REED_DEBOUNCE_MS 30UL
#endif

static const unsigned long STUCK_SENSOR_MS = 600000UL;
static const uint8_t  I2C_MAX_RETRIES = 3;

// [FIX-11] Anti-slam
static const float    VFD_STOPPED_HZ          = 0.5f;
static const unsigned long VFD_STOP_TIMEOUT_MS = 8000UL;

// [FIX-12] Extended telemetry poll interval (parameters only)
static const unsigned long VFD_EXT_POLL_INTERVAL_MS = 5000UL;
static unsigned long nextVfdExtPollMs = 0;

// ======================================================================
// GENERIC HELPERS
// ======================================================================

inline bool timeReached(unsigned long t, unsigned long now) {
  return (long)(now - t) >= 0;
}
inline unsigned long uptimeSecNow() { return millis() / 1000UL; }
inline unsigned long dayIndexNow()  { return uptimeSecNow() / 86400UL; }

void fmtUptimeBuf(unsigned long sec, char* buf, size_t sz) {
  unsigned long d = sec / 86400UL; sec %= 86400UL;
  unsigned long h = sec / 3600UL;  sec %= 3600UL;
  unsigned long m = sec / 60UL;
  if (d > 0) snprintf(buf, sz, "%lud %02lu:%02lu", d, h, m);
  else       snprintf(buf, sz, "%02lu:%02lu", h, m);
}

String fmtUptime(unsigned long sec) {
  char buf[24];
  fmtUptimeBuf(sec, buf, sizeof(buf));
  return String(buf);
}

static bool strHasValue(const char* s) { return s && s[0] != '\0'; }

static String base64Encode(const char* input) {
  static const char alphabet[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  String out;
  if (!input) return out;
  size_t len = strlen(input);
  out.reserve(((len + 2) / 3) * 4);
  for (size_t i = 0; i < len; i += 3) {
    bool has2 = (i + 1) < len, has3 = (i + 2) < len;
    uint32_t chunk = ((uint32_t)(uint8_t)input[i]) << 16;
    if (has2) chunk |= ((uint32_t)(uint8_t)input[i + 1]) << 8;
    if (has3) chunk |= (uint32_t)(uint8_t)input[i + 2];
    out += alphabet[(chunk >> 18) & 0x3F];
    out += alphabet[(chunk >> 12) & 0x3F];
    out += (has2 ? alphabet[(chunk >> 6) & 0x3F] : '=');
    out += (has3 ? alphabet[chunk & 0x3F] : '=');
  }
  return out;
}

static bool smtpReadResponse(Client& client, int expectedCode,
                             unsigned long timeoutMs = 8000UL) {
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
          if (!more) return code == expectedCode;
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
  client.print(cmd); client.print("\r\n");
  return smtpReadResponse(client, expectedCode);
}

// ======================================================================
// [FIX-5] I2C EXPANSION HELPERS
// ======================================================================

static unsigned long i2cErrorCount = 0;

static bool safeUpdateDigitalInputs(uint8_t expIdx) {
  for (uint8_t attempt = 0; attempt < I2C_MAX_RETRIES; ++attempt) {
    DigitalMechExpansion exp = OptaController.getExpansion(expIdx);
    exp.updateDigitalInputs();
    return true;
  }
  i2cErrorCount++;
  return false;
}

// ======================================================================
// SECTION 1: TOWERS (Expansion 0)
// ======================================================================

static const float TOWER_TRAY_TIME_K           = 9500.0f;
static const unsigned long MIN_TRAY_INTERVAL_MS = 60000UL;

struct Tower {
  Chan reed;
  Chan out;

  PinStatus     rawReed          = LOW;
  unsigned long rawChangeMs      = 0;
  PinStatus     stableReed       = LOW;

  unsigned long lastEdgeMs       = 0;
  unsigned long prevEdgeMs       = 0;
  unsigned long lastIntervalMs   = 0;
  unsigned long lastChangeMs     = 0;

  float         speedHz          = PRESETS[0].speed;
  unsigned long interval_ms      = PRESETS[0].interval_ms;

  bool          contactorOn      = false;
  bool          fault            = false;
  unsigned long inhibitUntilMs   = 0;
  bool          bypassLine       = false;

  unsigned long intervalHistory[INTERVAL_HISTORY_SIZE];
  uint8_t       intervalHistIdx  = 0;
  uint8_t       intervalHistCnt  = 0;

  unsigned long traysSinceActive = 0;

  bool          desiredContactorOn = false;

  bool          faultEmailSent         = false;
  unsigned long faultTriggeredMs       = 0;
  unsigned long faultExpectedIntervalMs = 0;
  bool          faultCommandedOn       = false;
  unsigned long lastFaultEmailAttempt  = 0;
};

static Tower towers[NUM_TOWERS];
unsigned long towerTrayCount[NUM_TOWERS];
unsigned int  towerIrrCount[NUM_TOWERS];

static const uint8_t  TRAYS_PER_TOWER       = 16;
static const uint8_t  NUM_IRR_BATCHES       = 2;
static const uint8_t  TOWERS_PER_IRR_BATCH  = 4;

// [FIX-15] SLOT-BASED IRRIGATION PROGRESSION
//
// 4 irrigation slots, each pairs a batch-0 tower with a batch-1 tower:
//   slot 0: T1 → T5       slot 1: T2 → T6
//   slot 2: T3 → T7       slot 3: T4 → T8
//
// Each slot runs ONE tower at a time.  When its current tower completes
// all required trays (TRAYS_PER_TOWER × irrigationCycles), the slot
// advances to the paired tower.  If a tower is offline (faulted, stopped,
// under maintenance), the slot skips it and tries the next candidate.
//
// Both batch toggles (T1-T4, T5-T8) are independent:
//   - Only batch 0 ON → slots run T1-T4 only
//   - Only batch 1 ON → slots run T5-T8 only
//   - Both ON → slots run T1 then T5, T2 then T6, etc.
//   - If T2 is down, slot 1 immediately starts T6 (no waiting)
//
// Cloud variables:
//   irrEnableBatch0   – toggle for towers 1-4
//   irrEnableBatch1   – toggle for towers 5-8
//   irrigationCycles  – number of full passes (1-10)

static const uint8_t NUM_IRR_SLOTS = 4;

static bool  irrBatch0Internal        = false;   // towers 1-4 toggle
static bool  irrBatch1Internal        = false;   // towers 5-8 toggle
static int   irrigationCyclesInternal = 1;       // 1-10

// Per-slot state
static int8_t irrSlotTower[NUM_IRR_SLOTS]    = {-1,-1,-1,-1};  // current tower idx, -1=idle
static bool   irrSlotComplete[NUM_IRR_SLOTS]  = {false,false,false,false};

// Total trays needed per tower = trays_per_cycle × number_of_cycles
static inline uint16_t traysNeededPerTower() {
  return (uint16_t)TRAYS_PER_TOWER * (uint16_t)irrigationCyclesInternal;
}

// Is ANY irrigation active?
static inline bool anyIrrigationActive() {
  return irrBatch0Internal || irrBatch1Internal;
}

// Can this tower be irrigated right now?
static bool irrTowerAvailable(uint8_t t) {
  if (t >= NUM_TOWERS) return false;
  // batch enabled?
  if (t < TOWERS_PER_IRR_BATCH  && !irrBatch0Internal) return false;
  if (t >= TOWERS_PER_IRR_BATCH && !irrBatch1Internal) return false;
  // tower must be running (not faulted, not stopped for maintenance)
  if (!towers[t].contactorOn) return false;
  // not already done
  if (towerIrrCount[t] >= traysNeededPerTower()) return false;
  return true;
}

// Advance a slot to its next available tower.
// Tries batch-0 candidate first (tower = slot), then batch-1 (tower = slot+4).
static void advanceIrrSlot(uint8_t slot) {
  uint8_t t0 = slot;               // batch 0 candidate
  uint8_t t1 = slot + NUM_IRR_SLOTS; // batch 1 candidate

  int8_t cur = irrSlotTower[slot];

  if (cur == -1) {
    // First assignment — try t0, then t1
    if (irrTowerAvailable(t0)) { irrSlotTower[slot] = t0; return; }
    if (irrTowerAvailable(t1)) { irrSlotTower[slot] = t1; return; }
  } else if (cur == (int8_t)t0) {
    // Finished or skipping t0 — advance to t1
    if (irrTowerAvailable(t1)) { irrSlotTower[slot] = t1; return; }
  }
  // else cur == t1 and it finished, or both unavailable

  // Both done or unavailable
  irrSlotTower[slot] = -1;
  irrSlotComplete[slot] = true;
}

// Is this channel the actively irrigating tower in its slot?
static inline bool channelActiveForIrr(uint8_t ch) {
  if (ch >= NUM_TOWERS) return true;  // non-tower always active
  uint8_t slot = ch % NUM_IRR_SLOTS;
  return irrSlotTower[slot] == (int8_t)ch;
}

// Check all slots for stale assignments (tower went offline mid-irrigation)
// and advance them.  Called periodically from loop().
static void checkIrrSlotsForOffline() {
  for (uint8_t s = 0; s < NUM_IRR_SLOTS; ++s) {
    int8_t cur = irrSlotTower[s];
    if (cur < 0) continue;  // idle
    if (!irrTowerAvailable((uint8_t)cur)) {
      Serial.print(F("[IRR] Slot ")); Serial.print(s);
      Serial.print(F(": T")); Serial.print(cur + 1);
      Serial.println(F(" went offline → advancing slot"));
      advanceIrrSlot(s);

      // Log new assignment
      if (irrSlotTower[s] >= 0) {
        Serial.print(F("[IRR] Slot ")); Serial.print(s);
        Serial.print(F(" → T")); Serial.println(irrSlotTower[s] + 1);
      } else {
        Serial.print(F("[IRR] Slot ")); Serial.print(s);
        Serial.println(F(" → idle (no more towers)"));
      }
    }
  }
}

// Are all slots complete?
static bool allIrrSlotsComplete() {
  for (uint8_t s = 0; s < NUM_IRR_SLOTS; ++s)
    if (!irrSlotComplete[s]) return false;
  return true;
}

// Initialize all slots (called when enabling a batch)
static void initIrrSlots() {
  for (uint8_t s = 0; s < NUM_IRR_SLOTS; ++s) {
    irrSlotTower[s] = -1;
    irrSlotComplete[s] = false;
    advanceIrrSlot(s);

    if (irrSlotTower[s] >= 0) {
      Serial.print(F("[IRR] Slot ")); Serial.print(s);
      Serial.print(F(" → T")); Serial.println(irrSlotTower[s] + 1);
    } else {
      Serial.print(F("[IRR] Slot ")); Serial.print(s);
      Serial.println(F(" → idle"));
    }
  }
}

// ======================================================================
// [FIX-11] VFD ANTI-SLAM STATE MACHINE
// ======================================================================

static const uint8_t NUM_VFD_GROUPS       = 2;
static const uint8_t TOWERS_PER_GROUP     = 4;

static inline uint8_t towerToGroup(uint8_t tIdx) {
  return (tIdx < 4) ? 0 : 1;
}
static inline uint8_t groupFirstTower(uint8_t g) { return g * TOWERS_PER_GROUP; }
static inline uint8_t groupLastTower(uint8_t g)  { return groupFirstTower(g) + TOWERS_PER_GROUP - 1; }

enum VfdGroupState : uint8_t {
  VGS_RUNNING,
  VGS_STOPPING,
  VGS_SWITCHING,
  VGS_RESTARTING
};

// ======================================================================
// [FIX-12] VFD EXTENDED TELEMETRY (SEW MOVITRAC LTE-B+)
// ======================================================================

struct VfdExtTelemetry {
  // Fast-polled (every 500ms alongside speed/status)
  float    motorCurrentPct    = 0.0f;   // PI3: actual current % of P-08

  // Derived
  float    peakCurrentPct     = 0.0f;   // Peak since daily reset

  // Slow-polled (every 5s – parameter registers)
  float    accelTimeSec       = 0.0f;   // P-03 (reg 131, addr 130)
  float    decelTimeSec       = 0.0f;   // P-04 (reg 132, addr 131)
  float    currentLimitPct    = 0.0f;   // P-54 (reg 182, addr 181)

  unsigned long modbusExtErrCount = 0;
  bool     extReadOk          = false;
};

struct VfdGroup {
  uint8_t       slaveId;
  VfdGroupState state            = VGS_RUNNING;
  unsigned long stateEnteredMs   = 0;
  bool          changesPending   = false;
  float         lastFeedbackHz   = 0.0f;
  float         prevFeedbackHz   = 0.0f;   // [FIX-13] For speed-change grace
  bool          commFault        = false;
  float         speedSetHz       = 0.0f;
  bool          runCmd           = false;

  VfdExtTelemetry ext;
};

static VfdGroup vfdGroups[NUM_VFD_GROUPS];

// Forward declarations
static bool  vfdWriteCommand(uint8_t slaveId, bool run, float speedHz);
static bool  vfdReadStatus(uint8_t slaveId, int16_t &statusWord,
                           float &speedHz, int16_t &alarmCode);
static bool  vfdReadMotorCurrent(uint8_t slaveId, float &currentPct);
static bool  vfdReadExtendedParams(uint8_t slaveId, VfdExtTelemetry &ext);
static bool  vfdWriteAccelTime(uint8_t slaveId, float seconds);

static bool groupHasPendingChanges(uint8_t g) {
  uint8_t first = groupFirstTower(g);
  for (uint8_t i = first; i <= groupLastTower(g); ++i) {
    if (towers[i].contactorOn != towers[i].desiredContactorOn)
      return true;
  }
  return false;
}

static bool groupAnyTowerShouldRun(uint8_t g) {
  uint8_t first = groupFirstTower(g);
  for (uint8_t i = first; i <= groupLastTower(g); ++i) {
    if (towers[i].desiredContactorOn) return true;
  }
  return false;
}

static void groupApplyContactorChanges(uint8_t g) {
  uint8_t first = groupFirstTower(g);
  for (uint8_t i = first; i <= groupLastTower(g); ++i) {
    Tower &t = towers[i];
    if (t.contactorOn != t.desiredContactorOn) {
      bool newState = t.desiredContactorOn;

      Serial.print(F("[ANTI-SLAM] Tower "));
      Serial.print(i + 1);
      Serial.print(F(" contactor "));
      Serial.println(newState ? "CLOSE" : "OPEN");

      DigitalMechExpansion exp = OptaController.getExpansion(t.out.exp);
      exp.digitalWrite(t.out.ch,
        (RELAY_ACTIVE_HIGH ? (newState ? HIGH : LOW) : (newState ? LOW : HIGH)));
      exp.updateDigitalOutputs();

      t.contactorOn    = newState;
      t.inhibitUntilMs = millis() + CHANGE_INHIBIT_MS;
    }
  }
}

static void requestContactorChange(uint8_t tIdx, bool on) {
  if (tIdx >= NUM_TOWERS) return;
  Tower &t = towers[tIdx];
  uint8_t g = towerToGroup(tIdx);
  VfdGroup &vg = vfdGroups[g];

  t.desiredContactorOn = on;

  if (t.contactorOn != on) {
    vg.changesPending = true;

    if (vg.state == VGS_RUNNING) {
      Serial.print(F("[ANTI-SLAM] Group "));
      Serial.print(g);
      Serial.println(F(" → STOPPING (contactor change requested)"));

      bool ok = vfdWriteCommand(vg.slaveId, false, vg.speedSetHz);
      vg.commFault      = !ok;
      vg.runCmd          = false;
      vg.state           = VGS_STOPPING;
      vg.stateEnteredMs  = millis();
    }
  }
}

static void processVfdGroupStateMachine(uint8_t g, unsigned long nowMs) {
  VfdGroup &vg = vfdGroups[g];

  switch (vg.state) {

    case VGS_RUNNING:
      if (groupHasPendingChanges(g)) {
        vg.changesPending = true;
        Serial.print(F("[ANTI-SLAM] Group "));
        Serial.print(g);
        Serial.println(F(" → STOPPING (pending change detected)"));
        vfdWriteCommand(vg.slaveId, false, vg.speedSetHz);
        vg.runCmd         = false;
        vg.state          = VGS_STOPPING;
        vg.stateEnteredMs = nowMs;
      }
      break;

    case VGS_STOPPING: {
      int16_t st = 0, al = 0;
      float sp = 0.0f;
      bool ok = vfdReadStatus(vg.slaveId, st, sp, al);
      vg.commFault = !ok;
      if (ok) vg.lastFeedbackHz = sp;

      bool speedOk = (ok && sp < VFD_STOPPED_HZ);
      bool timedOut = (nowMs - vg.stateEnteredMs) >= VFD_STOP_TIMEOUT_MS;

      if (speedOk || timedOut) {
        if (timedOut && !speedOk) {
          Serial.print(F("[ANTI-SLAM] WARNING: Group "));
          Serial.print(g);
          Serial.print(F(" stop timeout (speed="));
          Serial.print(ok ? sp : -1.0f, 1);
          Serial.println(F(" Hz). Forcing contactor switch."));
        } else {
          Serial.print(F("[ANTI-SLAM] Group "));
          Serial.print(g);
          Serial.println(F(" speed confirmed ≈0 → SWITCHING"));
        }
        vg.state          = VGS_SWITCHING;
        vg.stateEnteredMs = nowMs;
      }
    } break;

    case VGS_SWITCHING:
      groupApplyContactorChanges(g);
      vg.changesPending = false;
      vg.state          = VGS_RESTARTING;
      vg.stateEnteredMs = nowMs;
      Serial.print(F("[ANTI-SLAM] Group "));
      Serial.print(g);
      Serial.println(F(" contactors changed → RESTARTING"));
      break;

    case VGS_RESTARTING: {
      if ((nowMs - vg.stateEnteredMs) < 50) break;

      bool needRun = groupAnyTowerShouldRun(g);
      if (needRun) {
        Serial.print(F("[ANTI-SLAM] Group "));
        Serial.print(g);
        Serial.println(F(" → VFD RUN"));
        bool ok = vfdWriteCommand(vg.slaveId, true, vg.speedSetHz);
        vg.commFault = !ok;
        vg.runCmd    = true;
      } else {
        Serial.print(F("[ANTI-SLAM] Group "));
        Serial.print(g);
        Serial.println(F(" → VFD stays STOPPED (no towers need to run)"));
        vg.runCmd = false;
      }

      if (groupHasPendingChanges(g)) {
        vg.state          = VGS_STOPPING;
        vg.stateEnteredMs = nowMs;
        if (needRun) {
          vfdWriteCommand(vg.slaveId, false, vg.speedSetHz);
          vg.runCmd = false;
        }
      } else {
        vg.state          = VGS_RUNNING;
        vg.stateEnteredMs = nowMs;
      }
    } break;
  }
}

// ---- Rolling interval helpers ----

static void clearIntervalHistory(uint8_t idx) {
  Tower &t = towers[idx];
  t.intervalHistIdx = 0;
  t.intervalHistCnt = 0;
  for (uint8_t j = 0; j < INTERVAL_HISTORY_SIZE; ++j)
    t.intervalHistory[j] = 0;
}

static void addIntervalToHistory(uint8_t idx, unsigned long intervalMs) {
  Tower &t = towers[idx];
  t.intervalHistory[t.intervalHistIdx] = intervalMs;
  t.intervalHistIdx = (t.intervalHistIdx + 1) % INTERVAL_HISTORY_SIZE;
  if (t.intervalHistCnt < INTERVAL_HISTORY_SIZE)
    t.intervalHistCnt++;
}

static unsigned long rollingAvgInterval(uint8_t idx) {
  const Tower &t = towers[idx];
  if (t.intervalHistCnt == 0) return 0;
  unsigned long sum = 0;
  for (uint8_t j = 0; j < t.intervalHistCnt; ++j)
    sum += t.intervalHistory[j];
  return sum / t.intervalHistCnt;
}

// ======================================================================
// SMTP SENDING
// ======================================================================

static bool sendTowerFaultEmail(const String& subject, const String& body) {
  if (!strHasValue(SMTP_SERVER)) return false;
  if (SMTP_USE_TLS) return false;

  const char* recipients[1];
  const char* names[1];
  size_t recipientCount = 0;
  if (strHasValue(FAULT_EMAIL_RECIPIENT_1_ADDR)) {
    recipients[recipientCount] = FAULT_EMAIL_RECIPIENT_1_ADDR;
    names[recipientCount]      = FAULT_EMAIL_RECIPIENT_1_NAME;
    recipientCount++;
  }
  if (recipientCount == 0) return false;

  EthernetClient client;
  String smtpPortStr = String(SMTP_PORT);
  smtpPortStr.trim();
  if (smtpPortStr.isEmpty()) return false;

  bool portIsNumeric = true;
  for (size_t i = 0; i < smtpPortStr.length(); ++i)
    if (!isDigit(smtpPortStr.charAt(i))) { portIsNumeric = false; break; }
  if (!portIsNumeric) return false;

  long parsedPort = smtpPortStr.toInt();
  if (parsedPort <= 0 || parsedPort > 65535) return false;
  const uint16_t smtpPort = static_cast<uint16_t>(parsedPort);

  Serial.print(F("SMTP → ")); Serial.print(SMTP_SERVER);
  Serial.print(F(":")); Serial.println(smtpPort);

  if (!client.connect(SMTP_SERVER, smtpPort)) return false;

  bool ok = smtpReadResponse(client, 220);
  if (ok) {
    if (!smtpCommand(client, F("EHLO tower-controller"), 250))
      ok = smtpCommand(client, F("HELO tower-controller"), 250);
  }
  if (!ok) { client.stop(); return false; }

  if (strHasValue(SMTP_USERNAME)) {
    if (!smtpCommand(client, F("AUTH LOGIN"), 334))  { client.stop(); return false; }
    if (!smtpCommand(client, base64Encode(SMTP_USERNAME), 334)) { client.stop(); return false; }
    if (!smtpCommand(client, base64Encode(SMTP_PASSWORD), 235)) { client.stop(); return false; }
  }

  {
    String mailFrom = String("MAIL FROM:<") + FAULT_EMAIL_FROM_ADDR + ">";
    if (!smtpCommand(client, mailFrom, 250)) { client.stop(); return false; }
  }
  for (size_t i = 0; i < recipientCount; ++i) {
    String rcpt = String("RCPT TO:<") + recipients[i] + ">";
    if (!smtpCommand(client, rcpt, 250)) { client.stop(); return false; }
  }
  if (!smtpCommand(client, F("DATA"), 354)) { client.stop(); return false; }

  client.print(F("From: "));
  if (strHasValue(FAULT_EMAIL_FROM_NAME)) {
    client.print(FAULT_EMAIL_FROM_NAME); client.print(F(" <"));
  } else client.print(F("<"));
  client.print(FAULT_EMAIL_FROM_ADDR); client.print(F(">\r\n"));

  client.print(F("To: "));
  for (size_t i = 0; i < recipientCount; ++i) {
    if (i) client.print(F(", "));
    if (strHasValue(names[i])) {
      client.print(names[i]); client.print(F(" <"));
    } else client.print(F("<"));
    client.print(recipients[i]); client.print(F(">"));
  }
  client.print(F("\r\n"));

  client.print(F("Subject: ")); client.print(subject);
  client.print(F("\r\nMIME-Version: 1.0\r\n"));
  client.print(F("Content-Type: text/plain; charset=utf-8\r\n"));
  client.print(F("Content-Transfer-Encoding: 8bit\r\n\r\n"));
  client.print(body);
  if (!body.endsWith("\r\n")) client.print(F("\r\n"));
  client.print(F(".\r\n"));

  if (!smtpReadResponse(client, 250)) { client.stop(); return false; }

  smtpCommand(client, F("QUIT"), 221);
  client.stop();
  Serial.println(F("SMTP fault email dispatched."));
  return true;
}

static void notifyTowerFault(uint8_t idx, bool firstAttempt, unsigned long nowMs) {
  if (idx >= NUM_TOWERS) return;
  Tower &t = towers[idx];
  if (t.faultEmailSent) return;

  unsigned long sincePulse = (t.lastEdgeMs == 0)
    ? (nowMs - t.faultTriggeredMs) : (nowMs - t.lastEdgeMs);
  unsigned long expected =
    t.faultExpectedIntervalMs ? t.faultExpectedIntervalMs : t.interval_ms;

  // [FIX-12] Include VFD current in fault email
  uint8_t grp = towerToGroup(idx);
  float currentPct = vfdGroups[grp].ext.motorCurrentPct;

  char subjBuf[80];
  snprintf(subjBuf, sizeof(subjBuf), "Tower %u fault: tray offline%s",
           (unsigned)(idx + 1), firstAttempt ? "" : " (retry)");

  String body;
  body.reserve(512);
  body += String("Tower ") + (idx + 1) + " did not report activity as expected.\r\n";
  body += String("Expected interval: ") + String(((float)expected) / 1000.0f, 1) + " s\r\n";
  if (t.lastEdgeMs == 0) body += "Last pulse: none since activation\r\n";
  else body += String("Last pulse age: ") + String(((float)sincePulse) / 1000.0f, 1) + " s\r\n";
  body += String("Trays since active: ") + String(t.traysSinceActive) + "\r\n";
  body += String("Rolling avg: ") + String(((float)rollingAvgInterval(idx)) / 1000.0f, 1) + " s\r\n";
  body += String("VFD interval: ") + String(((float)t.interval_ms) / 1000.0f, 1) + " s\r\n";
  body += String("Speed (Hz): ") + String(t.speedHz, 1) + "\r\n";
  body += String("Motor current: ") + String(currentPct, 1) + "%\r\n";
  body += String("Peak current: ") + String(vfdGroups[grp].ext.peakCurrentPct, 1) + "%\r\n\r\n";
  body += "Please inspect the tray and acknowledge the fault.\r\n";

  bool sent = sendTowerFaultEmail(String(subjBuf), body);
  t.lastFaultEmailAttempt = nowMs;
  if (sent) t.faultEmailSent = true;
}

// ---- Relay helpers ----

static inline void setRelayChan(const Chan& c, bool on) {
  DigitalMechExpansion exp = OptaController.getExpansion(c.exp);
  exp.digitalWrite(c.ch,
    (RELAY_ACTIVE_HIGH ? (on ? HIGH : LOW) : (on ? LOW : HIGH)));
  exp.updateDigitalOutputs();
}

// ---- Tower management functions ----

void applyPresetToAll(uint8_t p) {
  if (p > 2) return;
  unsigned long now = millis();
  for (uint8_t i = 0; i < NUM_TOWERS; i++) {
    towers[i].speedHz        = PRESETS[p].speed;
    towers[i].interval_ms    = PRESETS[p].interval_ms;
    towers[i].inhibitUntilMs = now + CHANGE_INHIBIT_MS;
  }
}

static void resetTowerLearningState(uint8_t tIdx, unsigned long now) {
  Tower &t = towers[tIdx];
  t.lastEdgeMs             = 0;
  t.prevEdgeMs             = 0;
  t.lastIntervalMs         = 0;
  t.traysSinceActive       = 0;
  t.inhibitUntilMs         = now + CHANGE_INHIBIT_MS;
  clearIntervalHistory(tIdx);
  towerTrayCount[tIdx]     = 0;
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
  t.traysSinceActive        = 0;
  clearIntervalHistory(tIdx);
}

void applyBypass(uint8_t tIdx, bool bypass) {
  if (tIdx >= NUM_TOWERS) return;
  Tower &t = towers[tIdx];
  t.bypassLine = bypass;

  if (bypass) {
    t.fault = false;
    t.desiredContactorOn = true;
    requestContactorChange(tIdx, true);
    Serial.print(F("[BYPASS ON] Tower ")); Serial.println(tIdx + 1);
  } else {
    resetTowerLearningState(tIdx, millis());
    towerIrrCount[tIdx] = 0;
    t.desiredContactorOn = false;
    requestContactorChange(tIdx, false);
    Serial.print(F("[BYPASS OFF] Tower "));
    Serial.print(tIdx + 1);
    Serial.print(F(" – learning reset, fault arms after "));
    Serial.print(LEARNING_TRAYS);
    Serial.println(F(" trays."));
  }
}

void setContactor(uint8_t tIdx, bool on) {
  if (tIdx >= NUM_TOWERS) return;
  Tower &t = towers[tIdx];

  if (t.bypassLine) return;
  if (t.fault && on) return;

  bool wasOff = !t.contactorOn;
  if (wasOff && on)
    resetTowerLearningState(tIdx, millis());

  requestContactorChange(tIdx, on);
}

unsigned long computeTowerIntervalMsFromHz(float hz) {
  if (hz <= 0.1f) return PRESETS[0].interval_ms;
  float sec = TOWER_TRAY_TIME_K / hz;
  if (sec < 60.0f)   sec = 60.0f;
  if (sec > 1200.0f) sec = 1200.0f;
  return (unsigned long)(sec * 1000.0f + 0.5f);
}

// ---- Debounce + fault detection ----

void readReedAndUpdate(uint8_t i, unsigned long now) {
  Tower &t = towers[i];

  DigitalMechExpansion exp = OptaController.getExpansion(REED_CH[i].exp);
  PinStatus raw = exp.digitalRead(REED_CH[i].ch);

  if (raw != t.rawReed) {
    t.rawReed     = raw;
    t.rawChangeMs = now;
  }

  if (raw != t.stableReed && (now - t.rawChangeMs) >= REED_DEBOUNCE_MS) {
    PinStatus prevStable = t.stableReed;
    t.stableReed   = raw;
    t.lastChangeMs = now;

    bool becameActive =
      (REED_ACTIVE_HIGH
       ? (raw == HIGH && prevStable == LOW)
       : (raw == LOW  && prevStable == HIGH));

    if (becameActive && t.contactorOn) {
      if (t.lastEdgeMs != 0) {
        unsigned long dt = now - t.lastEdgeMs;

        if (dt < MIN_TRAY_INTERVAL_MS) return;

        t.prevEdgeMs     = t.lastEdgeMs;
        t.lastIntervalMs = dt;
        t.lastEdgeMs     = now;
        addIntervalToHistory(i, dt);
        t.traysSinceActive++;
        towerTrayCount[i]++;

        Serial.print(F("[REED] T")); Serial.print(i+1);
        Serial.print(F(" tray=")); Serial.print(t.traysSinceActive);
        Serial.print(F(" int=")); Serial.print(dt);
        Serial.print(F(" avg=")); Serial.println(rollingAvgInterval(i));

      } else {
        t.lastEdgeMs = now;
        t.traysSinceActive++;
        towerTrayCount[i]++;
      }
    }
  }

  // ---- FAULT DETECTION ---- [FIX-13 hardened]
  if (!t.contactorOn || t.fault || t.bypassLine || now <= t.inhibitUntilMs)
    return;

  if (t.traysSinceActive < LEARNING_TRAYS)
    return;

  unsigned long vfdInterval = t.interval_ms;
  unsigned long avgMeasured = rollingAvgInterval(i);

  // [FIX-13] Use rolling average as baseline ONLY when we have a full
  // history buffer (4 samples).  With fewer samples a single slow tray
  // skews the average and triggers a false positive on the next tray.
  unsigned long baseInterval;
  if (avgMeasured > 0 && t.intervalHistCnt >= INTERVAL_HISTORY_SIZE)
    baseInterval = (vfdInterval > avgMeasured) ? vfdInterval : avgMeasured;
  else
    baseInterval = vfdInterval;

  // [FIX-13] Tolerance: 50% normal (was 30% — too tight for CV=30-35% towers).
  // If Config.h LATE_TOLERANCE is still the old 0.30, override to 0.50 here.
  float normalTolerance = LATE_TOLERANCE;
  if (normalTolerance < 0.50f) normalTolerance = 0.50f;

  float tolerance = (t.traysSinceActive < (unsigned long)(LEARNING_TRAYS * 2))
    ? LEARNING_LATE_TOLERANCE : normalTolerance;

  unsigned long lateThresh = (unsigned long)(baseInterval * (1.0f + tolerance));
  unsigned long sincePulse =
    (t.lastEdgeMs == 0) ? (now - t.lastChangeMs) : (now - t.lastEdgeMs);

  if (sincePulse > lateThresh) {
    bool wasOn = t.contactorOn;

    Serial.print(F("[FAULT] T")); Serial.print(i+1);
    Serial.print(F(" late=")); Serial.print(sincePulse);
    Serial.print(F(" thresh=")); Serial.print(lateThresh);
    Serial.print(F(" base=")); Serial.print(baseInterval);
    Serial.print(F(" tol=")); Serial.print(tolerance, 2);
    Serial.print(F(" trays=")); Serial.println(t.traysSinceActive);

    t.fault                   = true;
    t.faultCommandedOn        = wasOn;
    t.faultExpectedIntervalMs = lateThresh;
    t.faultTriggeredMs        = now - sincePulse;
    t.faultEmailSent          = false;
    t.lastFaultEmailAttempt   = 0;

    t.desiredContactorOn = false;
    requestContactorChange(i, false);
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
static const PinStatus  ACTIVE_LEVEL   = HIGH;
static const bool       TRIGGER_ON_BOOT_HIGH = false;
static const unsigned long MIN_IRR_TRIGGER_MS = 30UL * 1000UL;

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

// [FIX-15] Slot-based helpers (slot infrastructure is above, near line 302)

unsigned long  inputActiveStartMs[NUM_CH];
bool           stuckSensorFault[NUM_CH];

static inline bool towerIrrigationComplete(uint8_t towerIdx) {
  return (towerIdx < NUM_TOWERS) && (towerIrrCount[towerIdx] >= traysNeededPerTower());
}
static inline bool anyIrrigationValveOn() {
  for (uint8_t ch = 0; ch < NUM_CH; ++ch)
    if (outStateArr[ch]) return true;
  return false;
}
static void forceStopAllIrrigation(DigitalMechExpansion& exp) {
  for (uint8_t ch = 0; ch < NUM_CH; ++ch) {
    exp.digitalWrite(ch, LOW);
    outStateArr[ch] = 0; offAtMs[ch] = 0;
    firedThisAssert[ch] = false;
    ignoreUntilMs[ch] = millis() + IGNORE_MS;
  }
  exp.updateDigitalOutputs();
}

bool          wasCloudConnected = false;
unsigned long disconnectStartMs = 0;
unsigned long lastUptimeBeforeDisconnectSec = 0;
unsigned long lastDayIndex = 0;

inline void fireValve(DigitalMechExpansion& exp, uint8_t ch, unsigned long now) {
  if (ch >= NUM_CH) return;
  const bool isTowerChannel = (ch < NUM_TOWERS);

  // [FIX-15] Only fire if this tower is the active tower in its slot
  if (isTowerChannel && !channelActiveForIrr(ch)) {
    firedThisAssert[ch] = true;
    ignoreUntilMs[ch]   = now + IGNORE_MS;
    return;
  }

  if (stuckSensorFault[ch]) {
    firedThisAssert[ch] = true;
    ignoreUntilMs[ch] = now + IGNORE_MS;
    return;
  }
  if (isTowerChannel && towerIrrigationComplete(ch)) {
    firedThisAssert[ch] = true;
    ignoreUntilMs[ch] = now + IGNORE_MS;
    return;
  }

  exp.digitalWrite(ch, HIGH);
  outStateArr[ch] = 1;
  offAtMs[ch] = now + PULSE_ON_MS;
  firedThisAssert[ch] = true;
  ignoreUntilMs[ch] = now + IGNORE_MS;
  exp.updateDigitalOutputs();

  if (isTowerChannel) {
    towerIrrCount[ch]++;

    // [FIX-15] Show cycle + slot progress
    uint8_t currentCycle = (towerIrrCount[ch] - 1) / TRAYS_PER_TOWER + 1;
    uint8_t trayInCycle  = ((towerIrrCount[ch] - 1) % TRAYS_PER_TOWER) + 1;
    uint8_t slot = ch % NUM_IRR_SLOTS;

    Serial.print(F("[IRR] T")); Serial.print(ch + 1);
    Serial.print(F(" valve #")); Serial.print(towerIrrCount[ch]);
    Serial.print(F("/")); Serial.print(traysNeededPerTower());
    Serial.print(F(" (cycle ")); Serial.print(currentCycle);
    Serial.print(F("/")); Serial.print(irrigationCyclesInternal);
    Serial.print(F(", tray ")); Serial.print(trayInCycle);
    Serial.print(F("/")); Serial.print(TRAYS_PER_TOWER);
    Serial.print(F(", slot ")); Serial.print(slot);
    Serial.println(F(")"));

    // Tower complete → advance slot to paired tower
    if (towerIrrigationComplete(ch)) {
      Serial.print(F("[IRR] T")); Serial.print(ch + 1);
      Serial.print(F(" complete (")); Serial.print(irrigationCyclesInternal);
      Serial.println(F(" cycle(s) done). Advancing slot..."));

      advanceIrrSlot(slot);

      if (irrSlotTower[slot] >= 0) {
        Serial.print(F("[IRR] Slot ")); Serial.print(slot);
        Serial.print(F(" → T")); Serial.println(irrSlotTower[slot] + 1);
      } else {
        Serial.print(F("[IRR] Slot ")); Serial.print(slot);
        Serial.println(F(" → done (both towers complete)"));
      }

      // [FIX-15] If all slots are done, auto-disable both batch toggles
      if (allIrrSlotsComplete()) {
        Serial.println(F("[IRR] All slots complete. Disabling irrigation."));
        irrBatch0Internal = false; irrEnableBatch0 = false;
        irrBatch1Internal = false; irrEnableBatch1 = false;

        forceStopAllIrrigation(exp);
      }
    }
  }
}

void publishIrrigationToCloud(unsigned long nowMs) {
  static unsigned long nextPublish = 0;
  if (!timeReached(nextPublish, nowMs)) return;

  inState0 = (lastStable[0] == ACTIVE_LEVEL);
  inState1 = (lastStable[1] == ACTIVE_LEVEL);
  inState2 = (lastStable[2] == ACTIVE_LEVEL);
  inState3 = (lastStable[3] == ACTIVE_LEVEL);
  inState4 = (lastStable[4] == ACTIVE_LEVEL);
  inState5 = (lastStable[5] == ACTIVE_LEVEL);
  inState6 = (lastStable[6] == ACTIVE_LEVEL);
  inState7 = (lastStable[7] == ACTIVE_LEVEL);

  outState0 = (outStateArr[0] != 0); outState1 = (outStateArr[1] != 0);
  outState2 = (outStateArr[2] != 0); outState3 = (outStateArr[3] != 0);
  outState4 = (outStateArr[4] != 0); outState5 = (outStateArr[5] != 0);
  outState6 = (outStateArr[6] != 0); outState7 = (outStateArr[7] != 0);

  trigCount0 = (int)trigCountArr[0]; trigCount1 = (int)trigCountArr[1];
  trigCount2 = (int)trigCountArr[2]; trigCount3 = (int)trigCountArr[3];
  trigCount4 = (int)trigCountArr[4]; trigCount5 = (int)trigCountArr[5];
  trigCount6 = (int)trigCountArr[6]; trigCount7 = (int)trigCountArr[7];

  auto toMin = [](unsigned long s){ return (int)(s / 60UL); };
  deltaMin0 = toMin(deltaSecArr[0]); deltaMin1 = toMin(deltaSecArr[1]);
  deltaMin2 = toMin(deltaSecArr[2]); deltaMin3 = toMin(deltaSecArr[3]);
  deltaMin4 = toMin(deltaSecArr[4]); deltaMin5 = toMin(deltaSecArr[5]);
  deltaMin6 = toMin(deltaSecArr[6]); deltaMin7 = toMin(deltaSecArr[7]);

  lastTrigAgoMin0 = (lastTrigMsArr[0]==0)?-1:(int)((nowMs-lastTrigMsArr[0])/60000UL);
  lastTrigAgoMin1 = (lastTrigMsArr[1]==0)?-1:(int)((nowMs-lastTrigMsArr[1])/60000UL);
  lastTrigAgoMin2 = (lastTrigMsArr[2]==0)?-1:(int)((nowMs-lastTrigMsArr[2])/60000UL);
  lastTrigAgoMin3 = (lastTrigMsArr[3]==0)?-1:(int)((nowMs-lastTrigMsArr[3])/60000UL);
  lastTrigAgoMin4 = (lastTrigMsArr[4]==0)?-1:(int)((nowMs-lastTrigMsArr[4])/60000UL);
  lastTrigAgoMin5 = (lastTrigMsArr[5]==0)?-1:(int)((nowMs-lastTrigMsArr[5])/60000UL);
  lastTrigAgoMin6 = (lastTrigMsArr[6]==0)?-1:(int)((nowMs-lastTrigMsArr[6])/60000UL);
  lastTrigAgoMin7 = (lastTrigMsArr[7]==0)?-1:(int)((nowMs-lastTrigMsArr[7])/60000UL);

  uptimeSec = (int)(nowMs / 1000UL);
  uptimeStr = fmtUptime(uptimeSec);

  IPAddress ip = Ethernet.localIP();
  if (ip) {
    char buf[24];
    snprintf(buf, sizeof(buf), "%u.%u.%u.%u", ip[0], ip[1], ip[2], ip[3]);
    ipAddressStr = String(buf);
  } else ipAddressStr = String("0.0.0.0");

  nextPublish = nowMs + 10000UL;
}

// Cloud callbacks
void onPulseSecondsChange() {
  if (pulseSeconds < 1)   pulseSeconds = 1;
  if (pulseSeconds > 600) pulseSeconds = 600;
  PULSE_ON_MS = (unsigned long)pulseSeconds * 1000UL;
  unsigned long now = millis();
  for (uint8_t ch = 0; ch < NUM_CH; ++ch)
    if (outStateArr[ch]) offAtMs[ch] = now + PULSE_ON_MS;
}

// [FIX-15] Initialize all irrigation channels and assign slots
static void initAllIrrigation(unsigned long now) {
  DigitalMechExpansion exp1 = OptaController.getExpansion(IRR_EXP_IDX);

  for (uint8_t ch = 0; ch < NUM_CH; ++ch) {
    towerIrrCount[ch] = 0;
    exp1.digitalWrite(ch, LOW);
    outStateArr[ch] = 0; offAtMs[ch] = 0;
    trigCountArr[ch] = 0; lastTrigMsArr[ch] = 0;
    prevTrigMsArr[ch] = 0; deltaSecArr[ch] = 0;
    PinStatus level = exp1.digitalRead(ch);
    lastRaw[ch] = level; lastStable[ch] = level;
    lastChangeMsIrr[ch] = now;
    firedThisAssert[ch] = (level == ACTIVE_LEVEL);
    ignoreUntilMs[ch] = 0;
    inputActiveStartMs[ch] = 0; stuckSensorFault[ch] = false;
  }
  exp1.updateDigitalOutputs();

  // Assign slots based on which batches are enabled
  initIrrSlots();

  Serial.print(F("[IRR] Irrigation started, "));
  Serial.print(irrigationCyclesInternal);
  Serial.print(F(" cycle(s) × ")); Serial.print(TRAYS_PER_TOWER);
  Serial.print(F(" trays = ")); Serial.print(traysNeededPerTower());
  Serial.println(F(" total per tower."));
}

// [FIX-15] Force-stop one batch's channels and re-evaluate slots
static void forceStopBatch(uint8_t batch) {
  DigitalMechExpansion exp1 = OptaController.getExpansion(IRR_EXP_IDX);
  uint8_t bStart = batch * TOWERS_PER_IRR_BATCH;
  uint8_t bEnd   = bStart + TOWERS_PER_IRR_BATCH;

  for (uint8_t ch = bStart; ch < bEnd && ch < NUM_CH; ++ch) {
    exp1.digitalWrite(ch, LOW);
    outStateArr[ch] = 0; offAtMs[ch] = 0;
    firedThisAssert[ch] = false;
    ignoreUntilMs[ch] = millis() + IGNORE_MS;
  }
  exp1.updateDigitalOutputs();

  Serial.print(F("[IRR] Batch ")); Serial.print(batch);
  Serial.println(F(" stopped."));

  // Re-evaluate all slots: any slot assigned to a tower in this batch
  // needs to advance (the tower is no longer available)
  for (uint8_t s = 0; s < NUM_IRR_SLOTS; ++s) {
    int8_t cur = irrSlotTower[s];
    if (cur < 0) continue;
    bool inThisBatch = (batch == 0) ? (cur < (int8_t)TOWERS_PER_IRR_BATCH)
                                    : (cur >= (int8_t)TOWERS_PER_IRR_BATCH);
    if (inThisBatch) {
      Serial.print(F("[IRR] Slot ")); Serial.print(s);
      Serial.print(F(": T")); Serial.print(cur + 1);
      Serial.println(F(" batch disabled → advancing"));
      advanceIrrSlot(s);
      if (irrSlotTower[s] >= 0) {
        Serial.print(F("[IRR] Slot ")); Serial.print(s);
        Serial.print(F(" → T")); Serial.println(irrSlotTower[s] + 1);
      } else {
        Serial.print(F("[IRR] Slot ")); Serial.print(s);
        Serial.println(F(" → idle"));
      }
    }
  }
}

void onIrrEnableBatch0Change() {
  bool wasEnabled = irrBatch0Internal;
  bool nowEnabled = irrEnableBatch0;
  irrBatch0Internal = nowEnabled;

  if (!nowEnabled) {
    forceStopBatch(0);
    return;
  }

  if (!wasEnabled && nowEnabled) {
    unsigned long now = millis();
    if (!irrBatch1Internal) {
      // First batch being enabled — full init
      initAllIrrigation(now);
    } else {
      // Batch 1 already running — just reset batch 0 channels and re-slot
      DigitalMechExpansion exp1 = OptaController.getExpansion(IRR_EXP_IDX);
      for (uint8_t ch = 0; ch < TOWERS_PER_IRR_BATCH; ++ch) {
        towerIrrCount[ch] = 0;
        exp1.digitalWrite(ch, LOW);
        outStateArr[ch] = 0; offAtMs[ch] = 0;
        firedThisAssert[ch] = false; ignoreUntilMs[ch] = 0;
        PinStatus level = exp1.digitalRead(ch);
        lastRaw[ch] = level; lastStable[ch] = level;
        lastChangeMsIrr[ch] = now;
        inputActiveStartMs[ch] = 0; stuckSensorFault[ch] = false;
      }
      exp1.updateDigitalOutputs();

      // Re-evaluate slots: any idle/complete slot might now pick up a batch-0 tower
      for (uint8_t s = 0; s < NUM_IRR_SLOTS; ++s) {
        if (irrSlotTower[s] < 0 && !irrSlotComplete[s]) {
          irrSlotComplete[s] = false;
          advanceIrrSlot(s);
        } else if (irrSlotComplete[s]) {
          // Slot was done — re-open it since batch 0 tower may be available
          irrSlotComplete[s] = false;
          advanceIrrSlot(s);
        }
      }

      Serial.println(F("[IRR] Batch 0 (T1-T4) added to running irrigation."));
    }
  }
}

void onIrrEnableBatch1Change() {
  bool wasEnabled = irrBatch1Internal;
  bool nowEnabled = irrEnableBatch1;
  irrBatch1Internal = nowEnabled;

  if (!nowEnabled) {
    forceStopBatch(1);
    return;
  }

  if (!wasEnabled && nowEnabled) {
    unsigned long now = millis();
    if (!irrBatch0Internal) {
      // First batch being enabled — full init
      initAllIrrigation(now);
    } else {
      // Batch 0 already running — just reset batch 1 channels and re-slot
      DigitalMechExpansion exp1 = OptaController.getExpansion(IRR_EXP_IDX);
      for (uint8_t ch = TOWERS_PER_IRR_BATCH; ch < NUM_TOWERS; ++ch) {
        towerIrrCount[ch] = 0;
        exp1.digitalWrite(ch, LOW);
        outStateArr[ch] = 0; offAtMs[ch] = 0;
        firedThisAssert[ch] = false; ignoreUntilMs[ch] = 0;
        PinStatus level = exp1.digitalRead(ch);
        lastRaw[ch] = level; lastStable[ch] = level;
        lastChangeMsIrr[ch] = now;
        inputActiveStartMs[ch] = 0; stuckSensorFault[ch] = false;
      }
      exp1.updateDigitalOutputs();

      // Re-evaluate slots: any idle/complete slot might now pick up a batch-1 tower
      for (uint8_t s = 0; s < NUM_IRR_SLOTS; ++s) {
        if (irrSlotTower[s] < 0 || irrSlotComplete[s]) {
          irrSlotComplete[s] = false;
          advanceIrrSlot(s);
        }
      }

      Serial.println(F("[IRR] Batch 1 (T5-T8) added to running irrigation."));
    }
  }
}

void onIrrigationCyclesChange() {
  if (irrigationCycles < 1)  irrigationCycles = 1;
  if (irrigationCycles > 10) irrigationCycles = 10;
  irrigationCyclesInternal = irrigationCycles;

  Serial.print(F("[IRR] Cycles set to ")); Serial.print(irrigationCyclesInternal);
  Serial.print(F(" (")); Serial.print(traysNeededPerTower());
  Serial.println(F(" trays/tower)."));

  // Re-evaluate slots: a tower that was "complete" under old count
  // might now be available (higher count) or a running tower might
  // now be complete (lower count).
  if (anyIrrigationActive()) {
    for (uint8_t s = 0; s < NUM_IRR_SLOTS; ++s) {
      int8_t cur = irrSlotTower[s];
      if (cur >= 0 && towerIrrigationComplete((uint8_t)cur)) {
        // Tower is now complete at new cycle count → advance
        Serial.print(F("[IRR] Slot ")); Serial.print(s);
        Serial.print(F(": T")); Serial.print(cur + 1);
        Serial.println(F(" now complete at new cycle count → advancing"));
        advanceIrrSlot(s);
      } else if (cur < 0 && !irrSlotComplete[s]) {
        // Slot was idle but not marked complete — re-try
        advanceIrrSlot(s);
      } else if (irrSlotComplete[s]) {
        // Slot was marked complete — re-open and try (higher cycle count
        // may have un-completed a tower)
        irrSlotComplete[s] = false;
        advanceIrrSlot(s);
      }
    }

    // If everything is now complete, auto-disable
    if (allIrrSlotsComplete()) {
      Serial.println(F("[IRR] All slots complete at new cycle count. Disabling."));
      irrBatch0Internal = false; irrEnableBatch0 = false;
      irrBatch1Internal = false; irrEnableBatch1 = false;
    }
  }
}

void onWindowStartSecChange() {}
void onWindowEndSecChange()   {}

// ======================================================================
// SECTION 3: TOWERS → CLOUD PUBLISH + BYPASS CALLBACKS
// ======================================================================

void publishTowersToCloud(unsigned long nowMs) {
  static unsigned long nextPublish = 0;
  if (!timeReached(nextPublish, nowMs)) return;

  auto edgeMin = [&](uint8_t i) -> int {
    if (towers[i].lastEdgeMs == 0) return -1;
    return (int)((nowMs - towers[i].lastEdgeMs) / 60000UL);
  };
  auto intervalMin = [&](uint8_t i) -> int {
    if (towers[i].lastIntervalMs == 0) return -1;
    return (int)(towers[i].lastIntervalMs / 60000UL);
  };

  tower0Running = towers[0].contactorOn;  tower0Fault = towers[0].fault;
  tower0LastEdgeMin = edgeMin(0);  tower0LastIntervalMin = intervalMin(0);
  tower0Bypass = towers[0].bypassLine;

  tower1Running = towers[1].contactorOn;  tower1Fault = towers[1].fault;
  tower1LastEdgeMin = edgeMin(1);  tower1LastIntervalMin = intervalMin(1);
  tower1Bypass = towers[1].bypassLine;

  tower2Running = towers[2].contactorOn;  tower2Fault = towers[2].fault;
  tower2LastEdgeMin = edgeMin(2);  tower2LastIntervalMin = intervalMin(2);
  tower2Bypass = towers[2].bypassLine;

  tower3Running = towers[3].contactorOn;  tower3Fault = towers[3].fault;
  tower3LastEdgeMin = edgeMin(3);  tower3LastIntervalMin = intervalMin(3);
  tower3Bypass = towers[3].bypassLine;

  tower4Running = towers[4].contactorOn;  tower4Fault = towers[4].fault;
  tower4LastEdgeMin = edgeMin(4);  tower4LastIntervalMin = intervalMin(4);
  tower4Bypass = towers[4].bypassLine;

  tower5Running = towers[5].contactorOn;  tower5Fault = towers[5].fault;
  tower5LastEdgeMin = edgeMin(5);  tower5LastIntervalMin = intervalMin(5);
  tower5Bypass = towers[5].bypassLine;

  tower6Running = towers[6].contactorOn;  tower6Fault = towers[6].fault;
  tower6LastEdgeMin = edgeMin(6);  tower6LastIntervalMin = intervalMin(6);
  tower6Bypass = towers[6].bypassLine;

  tower7Running = towers[7].contactorOn;  tower7Fault = towers[7].fault;
  tower7LastEdgeMin = edgeMin(7);  tower7LastIntervalMin = intervalMin(7);
  tower7Bypass = towers[7].bypassLine;

  tower0TrayCount = towerTrayCount[0]; tower1TrayCount = towerTrayCount[1];
  tower2TrayCount = towerTrayCount[2]; tower3TrayCount = towerTrayCount[3];
  tower4TrayCount = towerTrayCount[4]; tower5TrayCount = towerTrayCount[5];
  tower6TrayCount = towerTrayCount[6]; tower7TrayCount = towerTrayCount[7];

  nextPublish = nowMs + 10000UL;
}

// ======================================================================
// [FIX-12] EXTENDED VFD TELEMETRY → CLOUD PUBLISH
// ======================================================================

void publishVfdTelemetryToCloud(unsigned long nowMs) {
  static unsigned long nextPublish = 0;
  if (!timeReached(nextPublish, nowMs)) return;

  // VFD1 (group 1, slaveId 1) → towers 5-8
  const VfdExtTelemetry &e1 = vfdGroups[1].ext;
  vfd1MotorCurrentPct  = e1.motorCurrentPct;
  vfd1PeakCurrentPct   = e1.peakCurrentPct;
  vfd1AccelTimeSec     = e1.accelTimeSec;
  vfd1DecelTimeSec     = e1.decelTimeSec;
  vfd1CurrentLimitPct  = e1.currentLimitPct;

  // VFD2 (group 0, slaveId 2) → towers 1-4
  const VfdExtTelemetry &e2 = vfdGroups[0].ext;
  vfd2MotorCurrentPct  = e2.motorCurrentPct;
  vfd2PeakCurrentPct   = e2.peakCurrentPct;
  vfd2AccelTimeSec     = e2.accelTimeSec;
  vfd2DecelTimeSec     = e2.decelTimeSec;
  vfd2CurrentLimitPct  = e2.currentLimitPct;

  nextPublish = nowMs + 10000UL;
}

void onTower0BypassChange() { applyBypass(0, tower0Bypass); }
void onTower1BypassChange() { applyBypass(1, tower1Bypass); }
void onTower2BypassChange() { applyBypass(2, tower2Bypass); }
void onTower3BypassChange() { applyBypass(3, tower3Bypass); }
void onTower4BypassChange() { applyBypass(4, tower4Bypass); }
void onTower5BypassChange() { applyBypass(5, tower5Bypass); }
void onTower6BypassChange() { applyBypass(6, tower6Bypass); }
void onTower7BypassChange() { applyBypass(7, tower7Bypass); }

// ======================================================================
// SECTION 4: VFD MODBUS/RS485  (SEW MOVITRAC LTE-B+)
// ======================================================================
//
// 0-BASED register addresses (manual register number minus 1).
// The SEW manual numbers registers starting at 1, but the ArduinoModbus
// library uses 0-based addressing.
// ======================================================================

const uint8_t VFD1_ID = 1;
const uint8_t VFD2_ID = 2;

// Process data registers (0-based)
const uint16_t REG_CMD_WORD     = 0;    // PO1: control word      (manual reg 1)
const uint16_t REG_SPEED_REF    = 1;    // PO2: setpoint speed    (manual reg 2)
const uint16_t REG_STATUS_WORD  = 5;    // PI1: status word       (manual reg 6)
const uint16_t REG_ACTUAL_SPEED = 6;    // PI2: actual speed      (manual reg 7)
const uint16_t REG_ACTUAL_CURR  = 7;    // PI3: actual current    (manual reg 8)
const uint16_t REG_MOTOR_TORQUE = 8;    // PI4: motor torque      (manual reg 9)

// Parameter registers (0-based)
const uint16_t REG_ACCEL_TIME      = 130;  // P-03 (manual reg 131)
const uint16_t REG_DECEL_TIME      = 131;  // P-04 (manual reg 132)
const uint16_t REG_CURRENT_LIMIT   = 181;  // P-54 (manual reg 182)

// SEW MOVITRAC LTE-B+ scaling constants:
//   Speed:   0x4000 (16384) = 100% of P-01 (max speed)
//   Current: 0x4000 (16384) = 100% of P-08 (rated motor current)
//   P-03/04: raw × 0.01 = seconds  (e.g. 500 raw = 5.00 s)
//   P-54:    raw × 0.1  = percent  (e.g. 1500 raw = 150.0%)
const float VFD_MAX_FREQ_HZ = 53.3f;   // Your P-01 setting — adjust if different
const float VFD_SCALE_FULL  = 16384.0f;

static const unsigned long VFD_POLL_INTERVAL_MS = 500;
static unsigned long nextVfdPollMs = 0;
static unsigned long modbusInterFrameUs = 0;

// ---- Speed encoding/decoding (SEW: 16384 = 100% of P-01) ----

inline uint16_t vfdEncodeSpeed(float hz) {
  if (hz < 0.0f) hz = 0.0f;
  if (hz > VFD_MAX_FREQ_HZ) hz = VFD_MAX_FREQ_HZ;
  return (uint16_t)round((hz / VFD_MAX_FREQ_HZ) * VFD_SCALE_FULL);
}

inline float vfdDecodeSpeed(uint16_t raw) {
  return ((float)raw / VFD_SCALE_FULL) * VFD_MAX_FREQ_HZ;
}

// ---- Current decoding (SEW: 16384 = 100% of P-08) ----

inline float vfdDecodeCurrent(uint16_t raw) {
  return ((float)raw / VFD_SCALE_FULL) * 100.0f;   // result in percent
}

// ---- Command write ----

static bool vfdWriteCommand(uint8_t slaveId, bool run, float speedHz) {
  // SEW control word: 0x0006 = run along ramp, 0x0000 = stop along P-24 ramp
  uint16_t cmd   = run ? 0x0006 : 0x0000;
  uint16_t speed = vfdEncodeSpeed(speedHz);
  if (!ModbusRTUClient.holdingRegisterWrite(slaveId, REG_SPEED_REF, speed))
    return false;
  if (!ModbusRTUClient.holdingRegisterWrite(slaveId, REG_CMD_WORD, cmd))
    return false;
  return true;
}

// ---- Status read (PI1 status + PI2 speed) ----

static bool vfdReadStatus(uint8_t slaveId, int16_t &statusWord,
                          float &speedHz, int16_t &alarmCode) {
  if (!ModbusRTUClient.requestFrom(slaveId, HOLDING_REGISTERS,
                                   REG_STATUS_WORD, 2))
    return false;
  if (ModbusRTUClient.available() < 2) return false;
  uint16_t rawStatus = (uint16_t)ModbusRTUClient.read();
  uint16_t rawSpeed  = (uint16_t)ModbusRTUClient.read();
  statusWord = (int16_t)rawStatus;
  speedHz    = vfdDecodeSpeed(rawSpeed);

  // SEW: fault info is in status word bit 5 + high byte
  alarmCode = (rawStatus & 0x0020) ? (int16_t)((rawStatus >> 8) & 0xFF) : 0;
  return true;
}

// ---- [FIX-12] Motor current read (PI3, reg 8 / addr 7) ----

static bool vfdReadMotorCurrent(uint8_t slaveId, float &currentPct) {
  if (!ModbusRTUClient.requestFrom(slaveId, HOLDING_REGISTERS,
                                   REG_ACTUAL_CURR, 1))
    return false;
  if (ModbusRTUClient.available() < 1) return false;
  uint16_t raw = (uint16_t)ModbusRTUClient.read();
  // SEW: 0x4000 (16384) = 100% of rated motor current (P-08)
  currentPct = vfdDecodeCurrent(raw);
  return true;
}

// ---- [FIX-12] Extended parameter read (P-03, P-04, P-54) ----

static bool vfdReadExtendedParams(uint8_t slaveId, VfdExtTelemetry &ext) {
  bool allOk = true;

  // P-03 and P-04 are contiguous: addr 130, 131
  if (ModbusRTUClient.requestFrom(slaveId, HOLDING_REGISTERS,
                                   REG_ACCEL_TIME, 2)) {
    if (ModbusRTUClient.available() >= 2) {
      uint16_t rawAccel = (uint16_t)ModbusRTUClient.read();   // P-03
      uint16_t rawDecel = (uint16_t)ModbusRTUClient.read();   // P-04
      // SEW scaling: raw × 0.01 = seconds
      ext.accelTimeSec = (float)rawAccel * 0.01f;
      ext.decelTimeSec = (float)rawDecel * 0.01f;
    } else { allOk = false; }
  } else { allOk = false; }

  delayMicroseconds(modbusInterFrameUs);

  // P-54 current limit: addr 181
  {
    long val = ModbusRTUClient.holdingRegisterRead(slaveId, REG_CURRENT_LIMIT);
    if (!ModbusRTUClient.lastError()) {
      // SEW scaling: raw × 0.1 = percent
      ext.currentLimitPct = (float)val * 0.1f;
    } else { allOk = false; }
  }

  ext.extReadOk = allOk;
  if (!allOk) ext.modbusExtErrCount++;

  return allOk;
}

// ---- [FIX-12] Write acceleration time to VFD (P-03) ----

static bool vfdWriteAccelTime(uint8_t slaveId, float seconds) {
  // Clamp to valid range: 0.00 – 600 s
  if (seconds < 0.0f)   seconds = 0.0f;
  if (seconds > 600.0f) seconds = 600.0f;
  // SEW scaling: seconds / 0.01 = raw value
  uint16_t raw = (uint16_t)round(seconds / 0.01f);

  Serial.print(F("[VFD-WRITE] Slave "));
  Serial.print(slaveId);
  Serial.print(F(" P-03 accel = "));
  Serial.print(seconds, 2);
  Serial.print(F(" s (raw="));
  Serial.print(raw);
  Serial.println(F(")"));

  // Use function code 06 (Write Single Register) – as per SEW manual §12.3.3
  bool ok = ModbusRTUClient.holdingRegisterWrite(slaveId, REG_ACCEL_TIME, raw);
  if (!ok) {
    Serial.print(F("[VFD-WRITE] FAILED slave "));
    Serial.println(slaveId);
  }
  return ok;
}

// ---- Update tower intervals from VFD feedback ----

static void updateTowerIntervalFromVfdFeedback() {
  bool have1 = (!vfdGroups[1].commFault && vfdGroups[1].lastFeedbackHz > 1.0f);
  bool have2 = (!vfdGroups[0].commFault && vfdGroups[0].lastFeedbackHz > 1.0f);
  if (!have1 && !have2) return;

  unsigned long nowMs = millis();

  // [FIX-13] Speed-change grace period per VFD group.
  // When VFD speed changes significantly (e.g. 4PM slowdown), the old
  // rolling average is stale. Clear it and extend inhibit so the fault
  // detector re-learns at the new speed without a false trip.
  for (uint8_t g = 0; g < NUM_VFD_GROUPS; ++g) {
    VfdGroup &vg = vfdGroups[g];
    if (vg.prevFeedbackHz > 1.0f && vg.lastFeedbackHz > 1.0f) {
      float delta = vg.lastFeedbackHz - vg.prevFeedbackHz;
      if (delta < 0) delta = -delta;  // abs
      if (delta >= SPEED_CHANGE_THRESHOLD_HZ) {
        unsigned long newInterval = computeTowerIntervalMsFromHz(vg.lastFeedbackHz);
        unsigned long grace = newInterval * 2;  // 2× the new tray interval

        Serial.print(F("[FIX-13] Speed change on group "));
        Serial.print(g);
        Serial.print(F(": ")); Serial.print(vg.prevFeedbackHz, 1);
        Serial.print(F(" → ")); Serial.print(vg.lastFeedbackHz, 1);
        Serial.print(F(" Hz. Grace period: ")); Serial.print(grace / 1000UL);
        Serial.println(F(" s"));

        uint8_t first = groupFirstTower(g);
        uint8_t last  = groupLastTower(g);
        for (uint8_t i = first; i <= last; ++i) {
          clearIntervalHistory(i);
          towers[i].traysSinceActive = 0;  // re-enter learning
          towers[i].inhibitUntilMs = nowMs + grace;
        }
      }
    }
    vg.prevFeedbackHz = vg.lastFeedbackHz;
  }

  unsigned long int1 = have1 ? computeTowerIntervalMsFromHz(vfdGroups[1].lastFeedbackHz) : 0;
  unsigned long int2 = have2 ? computeTowerIntervalMsFromHz(vfdGroups[0].lastFeedbackHz) : 0;

  for (uint8_t i = 0; i < NUM_TOWERS; ++i) {
    if (i <= 3) {
      if (have2) { towers[i].speedHz = vfdGroups[0].lastFeedbackHz; towers[i].interval_ms = int2; }
    } else {
      if (have1) { towers[i].speedHz = vfdGroups[1].lastFeedbackHz; towers[i].interval_ms = int1; }
    }
  }
}

// ---- Cloud callbacks for VFD speed/run ----

void onVfd1SpeedSetHzChange() {
  vfdGroups[1].speedSetHz = vfd1SpeedSetHz;
  if (vfdGroups[1].state == VGS_RUNNING && vfdGroups[1].runCmd) {
    bool ok = vfdWriteCommand(VFD1_ID, true, vfd1SpeedSetHz);
    vfdGroups[1].commFault = !ok;
  }
}
void onVfd2SpeedSetHzChange() {
  vfdGroups[0].speedSetHz = vfd2SpeedSetHz;
  if (vfdGroups[0].state == VGS_RUNNING && vfdGroups[0].runCmd) {
    bool ok = vfdWriteCommand(VFD2_ID, true, vfd2SpeedSetHz);
    vfdGroups[0].commFault = !ok;
  }
}
void onVfd1RunCmdChange() {
  vfdGroups[1].runCmd = vfd1RunCmd;
  if (vfdGroups[1].state == VGS_RUNNING) {
    bool ok = vfdWriteCommand(VFD1_ID, vfd1RunCmd, vfdGroups[1].speedSetHz);
    vfdGroups[1].commFault = !ok;
  }
}
void onVfd2RunCmdChange() {
  vfdGroups[0].runCmd = vfd2RunCmd;
  if (vfdGroups[0].state == VGS_RUNNING) {
    bool ok = vfdWriteCommand(VFD2_ID, vfd2RunCmd, vfdGroups[0].speedSetHz);
    vfdGroups[0].commFault = !ok;
  }
}
void onVfd2AlarmCodeChange() {}

// ---- [FIX-12] Cloud callbacks for acceleration time (R/W) ----

void onVfd1AccelTimeSecChange() {
  float sec = vfd1AccelTimeSec;
  if (sec < 0.0f)   sec = 0.0f;
  if (sec > 600.0f) sec = 600.0f;
  vfd1AccelTimeSec = sec;   // clamp back

  Serial.print(F("[CLOUD] VFD1 accel time set to "));
  Serial.print(sec, 2);
  Serial.println(F(" s"));

  bool ok = vfdWriteAccelTime(VFD1_ID, sec);
  if (ok) {
    vfdGroups[1].ext.accelTimeSec = sec;   // update local cache
  } else {
    vfdGroups[1].commFault = true;
  }
}

void onVfd2AccelTimeSecChange() {
  float sec = vfd2AccelTimeSec;
  if (sec < 0.0f)   sec = 0.0f;
  if (sec > 600.0f) sec = 600.0f;
  vfd2AccelTimeSec = sec;

  Serial.print(F("[CLOUD] VFD2 accel time set to "));
  Serial.print(sec, 2);
  Serial.println(F(" s"));

  bool ok = vfdWriteAccelTime(VFD2_ID, sec);
  if (ok) {
    vfdGroups[0].ext.accelTimeSec = sec;
  } else {
    vfdGroups[0].commFault = true;
  }
}

// ---- VFD polling ----

static void pollVfdsIfDue(unsigned long nowMs) {
  // ---- FAST POLL: speed, status, motor current ----
  if ((long)(nowMs - nextVfdPollMs) >= 0) {

    for (uint8_t g = 0; g < NUM_VFD_GROUPS; ++g) {
      VfdGroup &vg = vfdGroups[g];
      if (vg.state != VGS_RUNNING) continue;

      // Read PI1 (status) + PI2 (speed)
      int16_t st = 0, al = 0;
      float sp = 0.0f;
      bool ok = vfdReadStatus(vg.slaveId, st, sp, al);
      vg.commFault = !ok;
      if (ok) vg.lastFeedbackHz = sp;

      delayMicroseconds(modbusInterFrameUs);

      // [FIX-12] Read PI3 (motor current)
      float curPct = 0.0f;
      bool curOk = vfdReadMotorCurrent(vg.slaveId, curPct);
      if (curOk) {
        vg.ext.motorCurrentPct = curPct;
        if (curPct > vg.ext.peakCurrentPct)
          vg.ext.peakCurrentPct = curPct;
      }

      // Update existing cloud variables
      if (g == 0) {  // VFD2
        vfd2CommFault  = vg.commFault;
        if (ok) { vfd2StatusWord = (int)st; vfd2SpeedFbHz = sp; vfd2AlarmCode = (int)al; }
      } else {       // VFD1
        vfd1CommFault  = vg.commFault;
        if (ok) { vfd1StatusWord = (int)st; vfd1SpeedFbHz = sp; vfd1AlarmCode = (int)al; }
      }

      delayMicroseconds(modbusInterFrameUs);
    }

    updateTowerIntervalFromVfdFeedback();
    nextVfdPollMs = nowMs + VFD_POLL_INTERVAL_MS;
  }

  // ---- SLOW POLL: extended parameters (P-03, P-04, P-54) ----
  if ((long)(nowMs - nextVfdExtPollMs) >= 0) {

    for (uint8_t g = 0; g < NUM_VFD_GROUPS; ++g) {
      VfdGroup &vg = vfdGroups[g];
      if (vg.state != VGS_RUNNING) continue;

      bool ok = vfdReadExtendedParams(vg.slaveId, vg.ext);

      if (ok) {
        Serial.print(F("[VFD-EXT] Slave "));
        Serial.print(vg.slaveId);
        Serial.print(F(": I=")); Serial.print(vg.ext.motorCurrentPct, 1);
        Serial.print(F("% Ipk=")); Serial.print(vg.ext.peakCurrentPct, 1);
        Serial.print(F("% Acc=")); Serial.print(vg.ext.accelTimeSec, 2);
        Serial.print(F("s Dec=")); Serial.print(vg.ext.decelTimeSec, 2);
        Serial.print(F("s Ilim=")); Serial.print(vg.ext.currentLimitPct, 1);
        Serial.println(F("%"));
      } else {
        Serial.print(F("[VFD-EXT] Slave "));
        Serial.print(vg.slaveId);
        Serial.print(F(" read error (total=")); Serial.print(vg.ext.modbusExtErrCount);
        Serial.println(F(")"));
      }

      delayMicroseconds(modbusInterFrameUs);
    }

    nextVfdExtPollMs = nowMs + VFD_EXT_POLL_INTERVAL_MS;
  }
}

// Debug helpers
void readDriveStatus(uint8_t id) {
  uint16_t status = ModbusRTUClient.holdingRegisterRead(id, REG_STATUS_WORD);
  if (!ModbusRTUClient.lastError()) {
    Serial.print("Drive "); Serial.print(id);
    Serial.print(" Status: 0x"); Serial.println(status, HEX);
  }
}

void vfdConnectionTestOnce() {
  Serial.println(F("\n===== VFD CONNECTION TEST ====="));
  readDriveStatus(VFD1_ID);
  readDriveStatus(VFD2_ID);
  Serial.println(F("===== DONE =====\n"));
}

// ======================================================================
// SECTION 5: SETUP & LOOP
// ======================================================================

static const float FIXED_TOWER_SPEED_HZ = 18.0f;

void setup() {
  Serial.begin(115200);
  OptaController.begin();
  Serial.println(F("OPTA combined v3: Towers + Irrigation + SEW MOVITRAC LTE-B+ VFDs"));

#if USE_HW_WATCHDOG
  IWatchdog.begin(4000000);
  Serial.println(F("Watchdog enabled (4s)."));
#endif

  // Cloud variable defaults
  ipAddressStr = String(""); lastOutageType = String("none"); uptimeStr = String("");
  vfd1SpeedFbHz = vfd1SpeedSetHz = vfd2SpeedFbHz = vfd2SpeedSetHz = 0.0f;
  deltaMin0=deltaMin1=deltaMin2=deltaMin3=deltaMin4=deltaMin5=deltaMin6=deltaMin7=0;
  lastOfflineDurationSec=0; lastReconnectUptimeSec=0;
  lastTrigAgoMin0=lastTrigAgoMin1=lastTrigAgoMin2=lastTrigAgoMin3=
  lastTrigAgoMin4=lastTrigAgoMin5=lastTrigAgoMin6=lastTrigAgoMin7=-1;
  networkLossCount=0; powerLossCount=0; pulseSeconds=40;
  // [FIX-14] Per-batch irrigation toggles (start disabled, user enables)
  irrEnableBatch0=false; irrEnableBatch1=false;
  irrigationCycles=1;
  tower0LastEdgeMin=tower0LastIntervalMin=tower1LastEdgeMin=tower1LastIntervalMin=
  tower2LastEdgeMin=tower2LastIntervalMin=tower3LastEdgeMin=tower3LastIntervalMin=
  tower4LastEdgeMin=tower4LastIntervalMin=tower5LastEdgeMin=tower5LastIntervalMin=
  tower6LastEdgeMin=tower6LastIntervalMin=tower7LastEdgeMin=tower7LastIntervalMin=-1;
  trigCount0=trigCount1=trigCount2=trigCount3=trigCount4=trigCount5=trigCount6=trigCount7=0;
  uptimeSec=0;
  vfd1AlarmCode=vfd1StatusWord=vfd2AlarmCode=vfd2StatusWord=0;
  inState0=inState1=inState2=inState3=inState4=inState5=inState6=inState7=false;
  irrBatch0Internal=irrEnableBatch0; irrBatch1Internal=irrEnableBatch1;
  irrigationCyclesInternal=irrigationCycles;
  outState0=outState1=outState2=outState3=outState4=outState5=outState6=outState7=false;
  tower0Bypass=tower1Bypass=tower2Bypass=tower3Bypass=
  tower4Bypass=tower5Bypass=tower6Bypass=tower7Bypass=false;
  tower0Fault=tower1Fault=tower2Fault=tower3Fault=
  tower4Fault=tower5Fault=tower6Fault=tower7Fault=false;
  tower0Running=tower1Running=tower2Running=tower3Running=
  tower4Running=tower5Running=tower6Running=tower7Running=false;
  vfd1CommFault=vfd2CommFault=false; vfd1RunCmd=vfd2RunCmd=false;
  tower0TrayCount=tower1TrayCount=tower2TrayCount=tower3TrayCount=
  tower4TrayCount=tower5TrayCount=tower6TrayCount=tower7TrayCount=0;

  // [FIX-12] Init new cloud variables
  vfd1MotorCurrentPct=vfd2MotorCurrentPct=0.0f;
  vfd1PeakCurrentPct=vfd2PeakCurrentPct=0.0f;
  vfd1AccelTimeSec=vfd2AccelTimeSec=0.0f;
  vfd1DecelTimeSec=vfd2DecelTimeSec=0.0f;
  vfd1CurrentLimitPct=vfd2CurrentLimitPct=0.0f;

  initProperties();
  ArduinoCloud.begin(ArduinoIoTPreferredConnection);

  PULSE_ON_MS = (unsigned long)pulseSeconds * 1000UL;
  lastDayIndex = dayIndexNow();
  lastOutageType = String("none");
  lastOfflineDurationSec=0; powerLossCount=0; networkLossCount=0;
  lastReconnectUptimeSec=0;

  // Init VFD groups
  vfdGroups[0].slaveId      = VFD2_ID;   // Group 0 = towers 0-3
  vfdGroups[0].state        = VGS_RUNNING;
  vfdGroups[0].speedSetHz   = FIXED_TOWER_SPEED_HZ;
  vfdGroups[0].runCmd       = false;
  vfdGroups[0].commFault    = false;
  vfdGroups[0].lastFeedbackHz = 0.0f;
  vfdGroups[0].prevFeedbackHz = 0.0f;

  vfdGroups[1].slaveId      = VFD1_ID;   // Group 1 = towers 4-7
  vfdGroups[1].state        = VGS_RUNNING;
  vfdGroups[1].speedSetHz   = FIXED_TOWER_SPEED_HZ;
  vfdGroups[1].runCmd       = false;
  vfdGroups[1].commFault    = false;
  vfdGroups[1].lastFeedbackHz = 0.0f;
  vfdGroups[1].prevFeedbackHz = 0.0f;

  // Towers init
  unsigned long nowMs = millis();
  for (uint8_t i = 0; i < NUM_TOWERS; i++) {
    towers[i].reed = REED_CH[i];
    towers[i].out  = OUT_CH[i];
    towers[i].rawReed = LOW; towers[i].rawChangeMs = nowMs;
    towers[i].stableReed = LOW;
    towers[i].lastEdgeMs=0; towers[i].prevEdgeMs=0;
    towers[i].lastIntervalMs=0; towers[i].lastChangeMs=nowMs;
    towers[i].fault=false; towers[i].faultEmailSent=false;
    towers[i].bypassLine=false; towers[i].contactorOn=false;
    towers[i].desiredContactorOn=false;
    towers[i].inhibitUntilMs = nowMs + CHANGE_INHIBIT_MS;
    towers[i].traysSinceActive=0;
    clearIntervalHistory(i);
    towerTrayCount[i]=0; towerIrrCount[i]=0;
  }

  // Default tower interval from 18 Hz
  {
    unsigned long intMs = computeTowerIntervalMsFromHz(FIXED_TOWER_SPEED_HZ);
    for (uint8_t i = 0; i < NUM_TOWERS; ++i) {
      towers[i].speedHz = FIXED_TOWER_SPEED_HZ;
      towers[i].interval_ms = intMs;
    }
    Serial.print(F("Default interval: ")); Serial.print(intMs);
    Serial.print(F(" ms (")); Serial.print(FIXED_TOWER_SPEED_HZ, 1);
    Serial.println(F(" Hz)"));
  }

  // Prime expansion inputs
  for (uint8_t e = 0; e < NUM_EXPANSIONS; ++e)
    safeUpdateDigitalInputs(e);

  for (uint8_t i = 0; i < NUM_TOWERS; i++) {
    DigitalMechExpansion ex = OptaController.getExpansion(REED_CH[i].exp);
    PinStatus level = ex.digitalRead(REED_CH[i].ch);
    towers[i].rawReed = level; towers[i].stableReed = level;
  }

  // Ensure tower outputs off at boot
  for (uint8_t i = 0; i < NUM_TOWERS; i++)
    setRelayChan(towers[i].out, false);

  tower0Bypass=towers[0].bypassLine; tower1Bypass=towers[1].bypassLine;
  tower2Bypass=towers[2].bypassLine; tower3Bypass=towers[3].bypassLine;
  tower4Bypass=towers[4].bypassLine; tower5Bypass=towers[5].bypassLine;
  tower6Bypass=towers[6].bypassLine; tower7Bypass=towers[7].bypassLine;

  // Irrigation init (exp1)
  DigitalMechExpansion exp1 = OptaController.getExpansion(IRR_EXP_IDX);
  for (uint8_t ch = 0; ch < NUM_CH; ++ch) {
    exp1.digitalWrite(ch, LOW);
    outStateArr[ch]=0; offAtMs[ch]=0; ignoreUntilMs[ch]=0;
    trigCountArr[ch]=0; lastTrigMsArr[ch]=0; prevTrigMsArr[ch]=0;
    deltaSecArr[ch]=0; firedThisAssert[ch]=false;
    inputActiveStartMs[ch]=0; stuckSensorFault[ch]=false;
  }
  exp1.updateDigitalOutputs();

  exp1.updateDigitalInputs();
  unsigned long nowIrr = millis();
  for (uint8_t ch = 0; ch < NUM_CH; ++ch) {
    lastRaw[ch] = exp1.digitalRead(ch);
    lastStable[ch] = lastRaw[ch];
    lastChangeMsIrr[ch] = nowIrr;
    if (TRIGGER_ON_BOOT_HIGH && lastStable[ch]==ACTIVE_LEVEL && anyIrrigationActive()) {
      trigCountArr[ch]=1; lastTrigMsArr[ch]=nowIrr;
      fireValve(exp1, ch, nowIrr);
    }
  }

  // RS485 / ModbusRTU
  // SEW default baud: 115200. Your Config.h may override.
  // Adjust VFD_BAUD below to match your P-36/2 setting.
  {
    const long VFD_BAUD = 19200;
    const float bitDuration = 1.0f / (float)VFD_BAUD;
    const float wordLen = 10.0f;
    const unsigned long preDelay = (unsigned long)(bitDuration * wordLen * 3.5f * 1e6f);
    modbusInterFrameUs = preDelay;

    RS485.begin(VFD_BAUD);
    RS485.setDelays(preDelay, preDelay);

    if (!ModbusRTUClient.begin(VFD_BAUD, SERIAL_8N1)) {
      Serial.println(F("Modbus RTU Client failed"));
      vfdGroups[0].commFault = vfdGroups[1].commFault = true;
    } else {
      Serial.println(F("Modbus RTU Client started"));
    }

    vfd1SpeedSetHz = FIXED_TOWER_SPEED_HZ;
    vfd2SpeedSetHz = FIXED_TOWER_SPEED_HZ;
    vfd1RunCmd = vfd2RunCmd = false;
  }

  // Ethernet
  if (Ethernet.begin(ETH_MAC) == 0) {
    Serial.println(F("DHCP failed; fallback IP"));
    IPAddress ip2(192,168,1,222), dns(192,168,1,1), gw(192,168,1,1), mask(255,255,255,0);
    Ethernet.begin(ETH_MAC, ip2, dns, gw, mask);
  }
  delay(200);
  IPAddress ip = Ethernet.localIP();
  Serial.print(F("IP: ")); Serial.println(ip);

  Serial.println(F("Setup complete."));
  Serial.println(F("VFD type: SEW MOVITRAC LTE-B+"));
  Serial.print(F("[FIX-13] Learning trays: ")); Serial.println(LEARNING_TRAYS);
  Serial.print(F("[FIX-13] History depth: ")); Serial.println(INTERVAL_HISTORY_SIZE);
  Serial.print(F("[FIX-13] Learning tolerance: ")); Serial.print(LEARNING_LATE_TOLERANCE*100.0f,0);
  Serial.println(F("%"));
  Serial.print(F("[FIX-13] Normal tolerance: ")); Serial.print((LATE_TOLERANCE < 0.50f ? 0.50f : LATE_TOLERANCE)*100.0f,0);
  Serial.println(F("% (min 50%)"));
  Serial.print(F("[FIX-13] Speed change threshold: ")); Serial.print(SPEED_CHANGE_THRESHOLD_HZ, 1);
  Serial.println(F(" Hz"));
  Serial.print(F("Anti-slam stop threshold: ")); Serial.print(VFD_STOPPED_HZ,1);
  Serial.println(F(" Hz"));
  Serial.print(F("Anti-slam stop timeout: ")); Serial.print(VFD_STOP_TIMEOUT_MS);
  Serial.println(F(" ms"));
  Serial.print(F("Extended VFD poll: ")); Serial.print(VFD_EXT_POLL_INTERVAL_MS);
  Serial.println(F(" ms"));
}

void loop() {
#if USE_HW_WATCHDOG
  IWatchdog.reload();
#endif

  ArduinoCloud.update();
  unsigned long nowMs = millis();

  // Daily reset
  unsigned long di = dayIndexNow();
  if (di != lastDayIndex) {
    for (uint8_t ch = 0; ch < NUM_CH; ++ch) trigCountArr[ch]=0;
    for (uint8_t i = 0; i < NUM_TOWERS; ++i) {
      towerTrayCount[i]=0;
      // [FIX-15] Only reset irrigation count if this tower is NOT actively assigned to a slot
      if (!channelActiveForIrr(i)) towerIrrCount[i]=0;
    }
    // [FIX-12] Reset daily peak current
    for (uint8_t g = 0; g < NUM_VFD_GROUPS; ++g)
      vfdGroups[g].ext.peakCurrentPct = 0.0f;

    lastDayIndex = di;
  }

  // Outage classification
  bool isConn = ArduinoCloud.connected();
  if (wasCloudConnected && !isConn) {
    disconnectStartMs = millis();
    lastUptimeBeforeDisconnectSec = uptimeSec;
  }
  if (!wasCloudConnected && isConn) {
    unsigned long nowSec = millis()/1000UL;
    bool powerLoss = (nowSec+5UL) < lastUptimeBeforeDisconnectSec;
    if (powerLoss) {
      powerLossCount++; lastOutageType=String("power"); lastOfflineDurationSec=0;
    } else {
      networkLossCount++; lastOutageType=String("network");
      lastOfflineDurationSec = disconnectStartMs ? (int)((millis()-disconnectStartMs)/1000UL) : 0;
    }
    lastReconnectUptimeSec = (int)nowSec;
    disconnectStartMs = 0;
  }
  wasCloudConnected = isConn;

  // Refresh expansion inputs
  for (uint8_t e = 0; e < NUM_EXPANSIONS; ++e)
    safeUpdateDigitalInputs(e);

  // Tower reed / fault logic
  for (uint8_t i = 0; i < NUM_TOWERS; i++)
    readReedAndUpdate(i, nowMs);

  // [FIX-4] Fault emails only when no irrigation valve active
  if (!anyIrrigationValveOn()) {
    for (uint8_t i = 0; i < NUM_TOWERS; ++i) {
      Tower &t = towers[i];
      if (!t.fault || t.faultEmailSent || t.bypassLine) continue;
      unsigned long lastAttempt = t.lastFaultEmailAttempt;
      if (lastAttempt==0 || (nowMs-lastAttempt) >= FAULT_EMAIL_RETRY_INTERVAL_MS)
        notifyTowerFault(i, (lastAttempt==0), nowMs);
    }
  }

  // Tower run control
  for (uint8_t i = 0; i < NUM_TOWERS; ++i) {
    if (towers[i].bypassLine) continue;
    bool desiredOn = !towers[i].fault;
    if (towers[i].desiredContactorOn != desiredOn)
      setContactor(i, desiredOn);
  }

  // Process VFD group state machines
  for (uint8_t g = 0; g < NUM_VFD_GROUPS; ++g)
    processVfdGroupStateMachine(g, nowMs);

  // Irrigation (exp1)
  DigitalMechExpansion exp1 = OptaController.getExpansion(IRR_EXP_IDX);

  for (uint8_t ch = 0; ch < NUM_CH; ++ch) {
    if (outStateArr[ch] && offAtMs[ch] && timeReached(offAtMs[ch], nowMs)) {
      exp1.digitalWrite(ch, LOW);
      outStateArr[ch]=0; offAtMs[ch]=0;
    }
  }
  exp1.updateDigitalOutputs();

  for (uint8_t ch = 0; ch < NUM_CH; ++ch) {
    PinStatus raw = exp1.digitalRead(ch);
    if (raw != lastRaw[ch]) { lastRaw[ch]=raw; lastChangeMsIrr[ch]=nowMs; }
    if ((nowMs-lastChangeMsIrr[ch]) < DEBOUNCE_MS) continue;
    if (raw == lastStable[ch]) continue;

    PinStatus prev = lastStable[ch];
    lastStable[ch] = raw;

    if (raw == ACTIVE_LEVEL) {
      if (inputActiveStartMs[ch]==0) inputActiveStartMs[ch]=nowMs;
    } else {
      inputActiveStartMs[ch]=0; stuckSensorFault[ch]=false;
    }

    if (prev==ACTIVE_LEVEL && raw!=ACTIVE_LEVEL) {
      firedThisAssert[ch]=false; continue;
    }

    if (prev!=ACTIVE_LEVEL && raw==ACTIVE_LEVEL) {
      if (ignoreUntilMs[ch] && !timeReached(ignoreUntilMs[ch], nowMs)) continue;
      if (lastTrigMsArr[ch]!=0 && (nowMs-lastTrigMsArr[ch]) < MIN_IRR_TRIGGER_MS) continue;

      trigCountArr[ch]++;
      deltaSecArr[ch] = (prevTrigMsArr[ch]==0) ? 0 : (nowMs-prevTrigMsArr[ch])/1000UL;
      prevTrigMsArr[ch]=nowMs; lastTrigMsArr[ch]=nowMs;

      if (anyIrrigationActive() && channelActiveForIrr(ch))
        fireValve(exp1, ch, nowMs);
      else {
        firedThisAssert[ch]=true; ignoreUntilMs[ch]=nowMs+IGNORE_MS;
      }
    }
  }

  // Stuck sensor check
  for (uint8_t ch = 0; ch < NUM_CH; ++ch) {
    if (lastStable[ch]==ACTIVE_LEVEL && inputActiveStartMs[ch]!=0 &&
        !stuckSensorFault[ch] && (nowMs-inputActiveStartMs[ch]) >= STUCK_SENSOR_MS) {
      stuckSensorFault[ch]=true;
      Serial.print(F("[STUCK] Irr ch ")); Serial.print(ch);
      Serial.println(F(" flagged."));
    }
  }

  // [FIX-15] Check for towers that went offline and advance their slots
  if (anyIrrigationActive()) {
    static unsigned long nextSlotCheck = 0;
    if (timeReached(nextSlotCheck, nowMs)) {
      checkIrrSlotsForOffline();
      nextSlotCheck = nowMs + 5000UL;  // check every 5s
    }
  }

  // Assist path
  if (anyIrrigationActive()) {
    for (uint8_t ch = 0; ch < NUM_CH; ++ch) {
      if (!channelActiveForIrr(ch)) continue;
      if (lastStable[ch]!=ACTIVE_LEVEL) continue;
      if (outStateArr[ch]) continue;
      if (firedThisAssert[ch]) continue;
      if (stuckSensorFault[ch]) continue;
      if (ignoreUntilMs[ch] && !timeReached(ignoreUntilMs[ch], nowMs)) continue;
      if (lastTrigMsArr[ch]==0) continue;
      fireValve(exp1, ch, nowMs);
    }
  }

  // VFD polling (fast + slow)
  pollVfdsIfDue(nowMs);

  // Cloud publishing
  publishIrrigationToCloud(nowMs);
  publishTowersToCloud(nowMs);
  publishVfdTelemetryToCloud(nowMs);
}
