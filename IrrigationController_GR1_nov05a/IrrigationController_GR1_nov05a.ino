#include "arduino_secrets.h"
#include "arduino_secrets.h"
// ======================================================================
//  TowerController_Combined_Refactored.ino  v4.3
//
//  CHANGELOG vs v4.2  (Oct 2026):
//
//  [FIX-27] PERIODIC RESETS: GRODAN API DISABLED (ENABLE_GRODAN 0)
//           GrodanSensors::update() ran 2-4 back-to-back HTTPS sessions
//           (DNS + TLS handshake + 5 s reply wait each) inside loop()
//           without feeding the 32.7 s IWDG. Over Starlink + the range
//           extender that chain could exceed the watchdog window, so the
//           Opta rebooted every few minutes, dropping every contactor and
//           restarting irrigation counts. It also froze reed/valve
//           handling for the duration. Disabled at compile time; the
//           ec/wc/temp variables stay registered and read 0. Do NOT
//           re-enable it inside loop() as is.
//
//  [FIX-28] VFD AMPS + TORQUE
//           PI3 (current) and PI4 (torque) are now read in ONE Modbus
//           transaction per drive (no extra bus time). Rated motor
//           current P-08 is read in the slow poll and used to convert
//           PI3 % to amps. New cloud variables (create in the Thing):
//             vfd1MotorCurrentA, vfd2MotorCurrentA      (float, Read Only)
//             vfd1MotorTorquePct, vfd2MotorTorquePct    (float, Read Only)
//           If P-08 reads wrong, set VFDx_RATED_CURRENT_A_OVERRIDE.
//
//  [FIX-29] IRRIGATION COUNT PROTECTION
//           - The uptime-based "day" rollover no longer zeroes the
//             per-tower irrigation count. It fired at a random time of
//             day, and if a tower was mid-run the count went back to 0
//             and that tower got a second full set of 16 trays. The
//             count is already reset when the operator re-arms the tower.
//           - Turning bypass OFF no longer zeroes the irrigation count
//             (bypass is a contactor function, not an irrigation one).
//
//  CHANGELOG vs v4.1  (offline resilience release, Oct 2026):
//
//  [FIX-18] VFD RUN-STATE RECONCILIATION  (root cause of "StoP" display)
//           The drive run word (PO1 = 0x0006) was only ever written from
//           a cloud callback or an anti-slam transition. If that single
//           write failed, or the drive stopped itself on its P-36 comm
//           timeout while the Opta was blocked in Ethernet.begin(), the
//           drive sat in "StoP" forever. Now every status poll compares
//           the drive's PI1 status word with the desired state and:
//             - re-sends RUN if we want RUN and the output stage is off
//             - sends STOP if we want STOP and it is running
//             - pulses the fault-reset bit (PO1 bit 6) if PI1 bit 5 is set
//           Rate limited (3 s re-assert, 30 s fault reset) so a drive
//           that is genuinely unable to run is not hammered.
//           Boot default is RUN (RUN_VFDS_AT_BOOT). Cloud vfdXRunCmd is
//           an operator override that still works when the cloud is up.
//           Towers run with no internet, no router and no DHCP.
//
//  [FIX-19] MODBUS BLOCKING CAPPED
//           ModbusRTUClient timeout 1000 ms -> 200 ms. A silent drive is
//           polled every 5 s (VFD_COMM_RETRY_MS) instead of every 500 ms,
//           so a dead RS485 link no longer starves the loop.
//
//  [FIX-20] ETHERNET OWNED BY THE CLOUD CONNECTION HANDLER
//           Removed the second Ethernet.begin() in setup(); two owners of
//           one interface was fragile. The handler's own begin() timeout
//           is cut from 15 s to 8 s and its retry interval raised from
//           0.5 s to 20 s, so a down network costs the loop at most 8 s
//           per 20 s instead of 15 s per 15.5 s. See NOTE below on the
//           static IP in arduino_secrets.h, which removes the DHCP wait.
//
//  [FIX-21] SMTP NO LONGER FREEZES THE LOOP
//           Fault e-mail is attempted only while the cloud is connected
//           (if the WAN is down the SMTP host is unreachable anyway),
//           per-response wait 8 s -> 3 s, the watchdog is kicked while
//           waiting, and failed attempts back off 5/10/20/40/80 min.
//
//  [FIX-22] WATCHDOG
//           ArduinoIoTCloud already arms the STM32H7 IWDG (32.7 s) and
//           kicks it inside ArduinoCloud.update(). Removed the dead
//           IWatchdog code (STM32duino API, does not build on the Opta
//           mbed core) and added wdKick() for use inside long operations.
//
//  [FIX-26] USE_STATIC_IP switch (default 0 = DHCP). TP-Link range
//           extenders only forward wired clients learned via DHCP; static
//           clients configure but get no traffic. Set to 1 once the Opta is
//           cabled to the Starlink router directly.
//
//  [FIX-25] Works around an Arduino_ConnectionHandler bug where a static
//           IPv4 passed to EthernetConnectionHandler's constructor is stored
//           in the wrong bytes and read back as 0.0.0.0 (so it silently
//           used DHCP). Settings are rewritten via updateSetting() so the
//           handler reads the real address. Serial prints what it will use.
//
//  [FIX-23] No reboot-on-cloud-loss. Deliberate: the plant must keep
//           running without internet, and a periodic reset would drop
//           every contactor. The IWDG covers true hangs.
//
//  NOTE - NETWORK CONFIG LIVES IN THIS FILE ([FIX-24]/[FIX-26] blocks):
//         DHCP by default (USE_STATIC_IP 0), static 192.168.1.250 when set
//         to 1. The generated NetworkConfigurator is bypassed either way. This builds unchanged in BOTH the
//         Cloud editor and the desktop IDE. SECRET_OPTIONAL_* are unused.
//
//  NOTE - Requires Arduino_ConnectionHandler >= 1.0 (ships with
//         ArduinoIoTCloud 2.x) for models::NetworkSetting / updateSetting().
//
//  CHANGELOG vs v4:
//  [FIX-17a] Removed cached TOWER8_PULSE_ON_MS — pulseDurationForTower()
//            now reads tower8PulseSeconds directly from the cloud variable
//            every call. Fixes a bug where the valve stayed on for the
//            old default (5 s) if the change callback didn't fire before
//            the next valve trigger.
//            Also added per-firing duration log so you can verify the
//            actual time applied.
//
//  CHANGELOG vs v3:
//  [FIX-17]  TOWER 8 INDEPENDENT IRRIGATION:
//            - Separate pulse duration: tower8PulseSeconds (default 5 s)
//              for Tower 8 only; Towers 1-7 keep using pulseSeconds (40 s).
//            - Configurable cycles per day: tower8IrrCyclesPerDay
//              (default 3). Total target trays = cycles × 16 (= 48 for T8).
//            - User toggles tower7IrrEnable to start a daily run; auto-
//              disables on completion. Daily reset zeroes the tray count
//              but NOT the enable toggle — operator re-arms each day.
//            - 48 trays fire back-to-back (no internal pause between cycles).
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
//  [FIX-16] PER-TOWER ON/OFF SWITCHES:
//           - Cloud toggles: tower0On .. tower7On (bool, R/W)
//           - Default: all ON at boot
//           - When OFF: contactor opens (via anti-slam), fault detection
//             suppressed for that tower
//           - When turned back ON: fault cleared, learning reset,
//             contactor re-closes after inhibit period
//           - Bypass overrides towerOn (bypass always wins)
//
//  [FIX-15] INTERMITTENT ROTATION MODE:
//           - Cloud toggle: intermittentMode (bool, R/W)
//           - When ON: 15 min running → 30 min rest → repeat
//           - VFDs ramp down via P-24 decel, ramp up via P-03 accel
//             so towers start/stop smoothly (no mechanical jerk)
//           - Fault detection suppressed during OFF phase and for a
//             60-second grace period after each restart
//           - Toggling intermittentMode OFF during rest phase
//             immediately resumes VFDs
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
//  [FIX-14] PER-TOWER IRRIGATION TOGGLES:
//           - Removed batch system (NUM_IRR_BATCHES, currentIrrBatch, etc.)
//           - Each tower has its own irrigationEnable toggle (cloud variable),
//             default false.
//           - Each tower independently counts trays; valve stops firing
//             after TRAYS_PER_TOWER (16) trays.
//           - Global irrigationEnable removed; replaced by per-tower
//             cloud booleans: towerXIrrEnable (0-7).
//           - Non-tower channels (ch >= NUM_TOWERS) still fire normally
//             when any tower irrigation is active.
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

// [FIX-27] Grodan cloud API. 0 = off (default). The fetch blocks loop()
// for tens of seconds and was resetting the Opta via the watchdog.
#ifndef ENABLE_GRODAN
#define ENABLE_GRODAN 0
#endif
#if ENABLE_GRODAN
const char GRODAN_USER[] = SECRET_GRODAN_USER;
const char GRODAN_PASS[] = SECRET_GRODAN_PASS;
#include "GrodanSensors.h"
#endif

// [FIX-22] Hardware watchdog. ArduinoIoTCloud arms the STM32H7 IWDG
// (32.7 s) in ArduinoCloud.begin() and kicks it in ArduinoCloud.update().
// wdKick() lets long blocking sections (SMTP) feed it too. Safe to call
// even if the watchdog was never started.
#include <mbed.h>

// Forward declarations for the Arduino preprocessor. It inserts auto-
// generated function prototypes above the first function in the file,
// so any helper that takes these structs by reference needs the names
// to exist here, before the structs themselves are defined.
struct Tower;
struct VfdGroup;

// ======================================================================
// [FIX-24] CLOUD-EDITOR SAFE NETWORK SETUP
//
// The Arduino Cloud editor regenerates thingProperties.h on every build with
//   EthernetConnectionHandler ArduinoIoTPreferredConnection;   (DHCP)
//   NetworkConfiguratorClass  NetworkConfigurator(...);
// and initProperties() calls ArduinoCloud.setConfigurator(NetworkConfigurator).
// On this site DHCP through the Wi-Fi extender is unreliable and the
// configurator keeps re-applying it, so the sketch:
//   - owns its own static-IP EthernetConnectionHandler (below)
//   - registers the cloud properties itself (initPropertiesStatic), which is
//     the generated list minus the configurator lines
//   - never calls the generated initProperties(), so setConfigurator() is
//     never called and the generated objects stay inert.
// The generated thingProperties.h is still compiled for the variable
// declarations and callback prototypes. If a cloud variable is ever added or
// removed in the Thing, copy the new addProperty line into
// initPropertiesStatic() below.
// ======================================================================
// [FIX-26] USE_STATIC_IP: 1 = static 192.168.1.250 (use when the Opta is
// cabled to the Starlink router). 0 = DHCP (REQUIRED while the Opta hangs
// off a TP-Link range extender: RE-series repeaters only forward traffic
// for wired clients they learned through DHCP, so a static client gets an
// address but no internet).
#ifndef USE_STATIC_IP
#define USE_STATIC_IP 0
#endif
static const IPAddress OPTA_STATIC_IP (192, 168, 1, 250);
static const IPAddress OPTA_DNS       (192, 168, 1, 1);
static const IPAddress OPTA_GATEWAY   (192, 168, 1, 1);
static const IPAddress OPTA_NETMASK   (255, 255, 255, 0);

EthernetConnectionHandler StaticEthConnection(
  OPTA_STATIC_IP, OPTA_DNS, OPTA_GATEWAY, OPTA_NETMASK);

static void initPropertiesStatic() {
  ArduinoCloud.addProperty(grodanStatus, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(ipAddressStr, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(lastOutageType, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(uptimeStr, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(ec108, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(ec119, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(ec120, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(temp108, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(temp119, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(temp120, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd1AccelTimeSec, READWRITE, ON_CHANGE, onVfd1AccelTimeSecChange);
  ArduinoCloud.addProperty(vfd1CurrentLimitPct, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd1DecelTimeSec, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd1MotorCurrentPct, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd1MotorCurrentA, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd1MotorTorquePct, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd1PeakCurrentPct, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd1SpeedFbHz, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd1SpeedSetHz, READWRITE, ON_CHANGE, onVfd1SpeedSetHzChange);
  ArduinoCloud.addProperty(vfd2AccelTimeSec, READWRITE, ON_CHANGE, onVfd2AccelTimeSecChange);
  ArduinoCloud.addProperty(vfd2CurrentLimitPct, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd2DecelTimeSec, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd2MotorCurrentPct, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd2MotorCurrentA, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd2MotorTorquePct, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd2PeakCurrentPct, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd2SpeedFbHz, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd2SpeedSetHz, READWRITE, ON_CHANGE, onVfd2SpeedSetHzChange);
  ArduinoCloud.addProperty(wc108, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(wc119, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(wc120, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(deltaMin0, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(deltaMin1, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(deltaMin2, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(deltaMin3, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(deltaMin4, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(deltaMin5, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(deltaMin6, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(deltaMin7, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(lastOfflineDurationSec, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(lastReconnectUptimeSec, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(lastTrigAgoMin0, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(lastTrigAgoMin1, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(lastTrigAgoMin2, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(lastTrigAgoMin3, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(lastTrigAgoMin4, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(lastTrigAgoMin5, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(lastTrigAgoMin6, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(lastTrigAgoMin7, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(networkLossCount, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(powerLossCount, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(pulseSeconds, READWRITE, ON_CHANGE, onPulseSecondsChange);
  ArduinoCloud.addProperty(tower0LastEdgeMin, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower0LastIntervalMin, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower0TrayCount, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower1LastEdgeMin, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower1LastIntervalMin, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower1TrayCount, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower2LastEdgeMin, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower2LastIntervalMin, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower2TrayCount, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower3LastEdgeMin, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower3LastIntervalMin, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower3TrayCount, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower4LastEdgeMin, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower4LastIntervalMin, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower4TrayCount, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower5LastEdgeMin, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower5LastIntervalMin, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower5TrayCount, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower6LastEdgeMin, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower6LastIntervalMin, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower6TrayCount, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower7LastEdgeMin, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower7LastIntervalMin, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower7TrayCount, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower8IrrCyclesPerDay, READWRITE, ON_CHANGE, onTower8IrrCyclesPerDayChange);
  ArduinoCloud.addProperty(tower8PulseSeconds, READWRITE, ON_CHANGE, onTower8PulseSecondsChange);
  ArduinoCloud.addProperty(trigCount0, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(trigCount1, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(trigCount2, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(trigCount3, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(trigCount4, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(trigCount5, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(trigCount6, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(trigCount7, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(uptimeSec, READ, ON_CHANGE, NULL, 60);
  ArduinoCloud.addProperty(vfd1AlarmCode, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd1StatusWord, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd2AlarmCode, READWRITE, ON_CHANGE, onVfd2AlarmCodeChange);
  ArduinoCloud.addProperty(vfd2StatusWord, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(inState0, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(inState1, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(inState2, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(inState3, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(inState4, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(inState5, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(inState6, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(inState7, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(intermittentMode, READWRITE, ON_CHANGE, onIntermittentModeChange);
  ArduinoCloud.addProperty(outState0, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(outState1, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(outState2, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(outState3, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(outState4, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(outState5, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(outState6, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(outState7, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower0Bypass, READWRITE, ON_CHANGE, onTower0BypassChange);
  ArduinoCloud.addProperty(tower0Fault, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower0IrrEnable, READWRITE, ON_CHANGE, onTower0IrrEnableChange);
  ArduinoCloud.addProperty(tower0On, READWRITE, ON_CHANGE, onTower0OnChange);
  ArduinoCloud.addProperty(tower0Running, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower1Bypass, READWRITE, ON_CHANGE, onTower1BypassChange);
  ArduinoCloud.addProperty(tower1Fault, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower1IrrEnable, READWRITE, ON_CHANGE, onTower1IrrEnableChange);
  ArduinoCloud.addProperty(tower1On, READWRITE, ON_CHANGE, onTower1OnChange);
  ArduinoCloud.addProperty(tower1Running, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower2Bypass, READWRITE, ON_CHANGE, onTower2BypassChange);
  ArduinoCloud.addProperty(tower2Fault, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower2IrrEnable, READWRITE, ON_CHANGE, onTower2IrrEnableChange);
  ArduinoCloud.addProperty(tower2On, READWRITE, ON_CHANGE, onTower2OnChange);
  ArduinoCloud.addProperty(tower2Running, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower3Bypass, READWRITE, ON_CHANGE, onTower3BypassChange);
  ArduinoCloud.addProperty(tower3Fault, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower3IrrEnable, READWRITE, ON_CHANGE, onTower3IrrEnableChange);
  ArduinoCloud.addProperty(tower3On, READWRITE, ON_CHANGE, onTower3OnChange);
  ArduinoCloud.addProperty(tower3Running, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower4Bypass, READWRITE, ON_CHANGE, onTower4BypassChange);
  ArduinoCloud.addProperty(tower4Fault, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower4IrrEnable, READWRITE, ON_CHANGE, onTower4IrrEnableChange);
  ArduinoCloud.addProperty(tower4On, READWRITE, ON_CHANGE, onTower4OnChange);
  ArduinoCloud.addProperty(tower4Running, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower5Bypass, READWRITE, ON_CHANGE, onTower5BypassChange);
  ArduinoCloud.addProperty(tower5Fault, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower5IrrEnable, READWRITE, ON_CHANGE, onTower5IrrEnableChange);
  ArduinoCloud.addProperty(tower5On, READWRITE, ON_CHANGE, onTower5OnChange);
  ArduinoCloud.addProperty(tower5Running, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower6Bypass, READWRITE, ON_CHANGE, onTower6BypassChange);
  ArduinoCloud.addProperty(tower6Fault, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower6IrrEnable, READWRITE, ON_CHANGE, onTower6IrrEnableChange);
  ArduinoCloud.addProperty(tower6On, READWRITE, ON_CHANGE, onTower6OnChange);
  ArduinoCloud.addProperty(tower6Running, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower7Bypass, READWRITE, ON_CHANGE, onTower7BypassChange);
  ArduinoCloud.addProperty(tower7Fault, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(tower7IrrEnable, READWRITE, ON_CHANGE, onTower7IrrEnableChange);
  ArduinoCloud.addProperty(tower7On, READWRITE, ON_CHANGE, onTower7OnChange);
  ArduinoCloud.addProperty(tower7Running, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd1CommFault, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd1RunCmd, READWRITE, ON_CHANGE, onVfd1RunCmdChange);
  ArduinoCloud.addProperty(vfd2CommFault, READ, ON_CHANGE, NULL);
  ArduinoCloud.addProperty(vfd2RunCmd, READWRITE, ON_CHANGE, onVfd2RunCmdChange);
}

static inline void wdKick() {
  mbed::Watchdog &wd = mbed::Watchdog::get_instance();
  if (wd.is_running()) wd.kick();
}

// [FIX-20] Tune the cloud connection handler's Ethernet timeouts from the
// sketch (needs Arduino_ConnectionHandler >= 1.0, shipped with
// ArduinoIoTCloud 2.x). Set to 0 and edit thingProperties.h instead if
// your library is older (see NOTE in the header).
#if USE_STATIC_IP
static const uint32_t ETH_BEGIN_TIMEOUT_MS   = 8000;   // static: only needs link-up
#else
static const uint32_t ETH_BEGIN_TIMEOUT_MS   = 12000;  // DHCP via repeater: discover/offer can take several s
#endif
static const uint32_t ETH_RESPONSE_TIMEOUT_MS = 3000;  // was 4000
static const uint32_t ETH_INIT_RETRY_MS      = 20000;  // was 500

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
// [FIX-21] Retry interval doubles per consecutive failure, up to << 4
// (5, 10, 20, 40, 80 min).
static const uint8_t  FAULT_EMAIL_MAX_BACKOFF_SHIFT = 4;
static const unsigned long SMTP_RESPONSE_TIMEOUT_MS = 3000UL;   // was 8000

// ======================================================================
// TUNING CONSTANTS
// ======================================================================

// [FIX-13] Tuning constants — data-driven values from Jan 2026 analysis.
static const uint8_t  LEARNING_TRAYS            = 5;
static const uint8_t  INTERVAL_HISTORY_SIZE     = 4;
static const float    LEARNING_LATE_TOLERANCE   = 1.00f;
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

// [FIX-18] Run-state reconciliation
static const bool          RUN_VFDS_AT_BOOT            = true;     // towers run with no cloud
static const unsigned long VFD_REASSERT_INTERVAL_MS    = 3000UL;   // min gap between RUN re-sends
static const unsigned long VFD_FAULT_RESET_INTERVAL_MS = 30000UL;  // min gap between reset pulses
static const unsigned long VFD_STATE_READ_MIN_GAP_MS   = 100UL;    // STOPPING-state read throttle

// [FIX-19] Modbus blocking caps
static const unsigned long VFD_MODBUS_TIMEOUT_MS = 200UL;    // library default is 1000
static const unsigned long VFD_COMM_RETRY_MS     = 5000UL;   // poll gap for a silent drive

// SEW MOVITRAC LTE-B+ PI1 status word bits (manual 12.1.1)
static const uint16_t PI1_OUTPUT_ENABLED = 0x0001;
static const uint16_t PI1_READY          = 0x0002;
static const uint16_t PI1_FAULT          = 0x0020;
// PO1 control word values (manual 12.1.2)
static const uint16_t PO1_STOP_RAPID     = 0x0000;   // stop along P-24
static const uint16_t PO1_RUN            = 0x0006;   // ramp along P-03 and run at PO2
static const uint16_t PO1_FAULT_RESET    = 0x0040;   // bit 6, rising edge resets

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
                             unsigned long timeoutMs = SMTP_RESPONSE_TIMEOUT_MS) {
  unsigned long start = millis();
  String line;
  while ((millis() - start) < timeoutMs) {
    wdKick();                                   // [FIX-21]
    if (!client.connected() && !client.available()) return false;
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
  uint8_t       faultEmailFails        = 0;     // [FIX-21] consecutive failures
};

static Tower towers[NUM_TOWERS];
unsigned long towerTrayCount[NUM_TOWERS];
unsigned int  towerIrrCount[NUM_TOWERS];

static const uint8_t  TRAYS_PER_TOWER       = 16;

// [FIX-14] Per-tower irrigation enable flags (driven by cloud variables).
static bool towerIrrEnabled[NUM_TOWERS];

// [FIX-16] Per-tower ON/OFF switches (driven by cloud variables).
static bool towerOnEnabled[NUM_TOWERS];

// ======================================================================
// [FIX-11] VFD ANTI-SLAM STATE MACHINE
// ======================================================================

static const uint8_t NUM_VFD_GROUPS       = 2;
static const uint8_t TOWERS_PER_GROUP     = 4;

// [FIX-15] intermittent phase flag (declared here because vfdWantsRun()
// below needs it)
static bool  intermittentPhaseOn     = true;

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
  float    motorCurrentPct    = 0.0f;
  float    peakCurrentPct     = 0.0f;
  float    motorCurrentA      = 0.0f;   // [FIX-28] PI3 % x rated amps
  float    motorTorquePct     = 0.0f;   // [FIX-28] PI4
  float    ratedCurrentA      = 0.0f;   // [FIX-28] P-08 (or override)
  float    accelTimeSec       = 0.0f;
  float    decelTimeSec       = 0.0f;
  float    currentLimitPct    = 0.0f;
  unsigned long modbusExtErrCount = 0;
  bool     extReadOk          = false;
};

struct VfdGroup {
  uint8_t       slaveId;
  VfdGroupState state            = VGS_RUNNING;
  unsigned long stateEnteredMs   = 0;
  bool          changesPending   = false;
  float         lastFeedbackHz   = 0.0f;
  float         prevFeedbackHz   = 0.0f;
  bool          commFault        = false;
  float         speedSetHz       = 0.0f;
  bool          runCmd           = false;

  // [FIX-18] reconciliation bookkeeping
  bool          driveEnabled     = false;   // PI1 bit 0 from last good read
  bool          driveFault       = false;   // PI1 bit 5 from last good read
  unsigned long lastReassertMs   = 0;
  unsigned long lastFaultResetMs = 0;
  unsigned long lastStateReadMs  = 0;
  unsigned long reassertCount    = 0;
  unsigned long faultResetCount  = 0;

  // [FIX-19] comm-fault backoff
  unsigned long nextCommRetryMs  = 0;

  VfdExtTelemetry ext;
};

static VfdGroup vfdGroups[NUM_VFD_GROUPS];

// [FIX-18] Does this group want the motor turning right now?
// runCmd is the operator/anti-slam intent; intermittent OFF phase is a
// deliberate temporary stop that must not be "fixed" by reconciliation.
static inline bool vfdWantsRun(const VfdGroup &vg) {
  if (!vg.runCmd) return false;
  if (intermittentMode && !intermittentPhaseOn) return false;
  return true;
}

// ======================================================================
// [FIX-15] INTERMITTENT ROTATION MODE
// ======================================================================

static const unsigned long INTERMITTENT_ON_MS  = 20UL * 60UL * 1000UL;
static const unsigned long INTERMITTENT_OFF_MS = 20UL * 60UL * 1000UL;
static const unsigned long INTERMITTENT_RESTART_GRACE_MS = 60000UL;

static unsigned long intermittentPhaseStartMs = 0;

// Forward declarations
static bool  vfdWriteCommand(uint8_t slaveId, bool run, float speedHz);
static bool  vfdReadStatus(uint8_t slaveId, int16_t &statusWord,
                           float &speedHz, int16_t &alarmCode);
static bool  vfdReadCurrentTorque(uint8_t slaveId, float &currentPct,
                                  float &torquePct);
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
      // [FIX-19] do not hammer the bus every loop iteration
      if ((nowMs - vg.lastStateReadMs) < VFD_STATE_READ_MIN_GAP_MS) break;
      vg.lastStateReadMs = nowMs;

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
      // [FIX-18] keep the dashboard switch in step with reality
      if (g == 0) vfd2RunCmd = vg.runCmd; else vfd1RunCmd = vg.runCmd;

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

  // [FIX-21] If the cloud is down the WAN almost certainly is too; a
  // blind SMTP connect() can block for tens of seconds. Skip it.
  if (!ArduinoCloud.connected()) {
    Serial.println(F("SMTP skipped: cloud offline"));
    return false;
  }

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

  wdKick();
  if (!client.connect(SMTP_SERVER, smtpPort)) return false;
  wdKick();

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
  if (sent) {
    t.faultEmailSent = true;
    t.faultEmailFails = 0;
  } else if (t.faultEmailFails < 255) {
    t.faultEmailFails++;                                   // [FIX-21]
  }
}

// [FIX-21] 5 min, 10, 20, 40, 80 min between attempts.
static inline unsigned long faultEmailRetryMs(const Tower &t) {
  uint8_t shift = t.faultEmailFails;
  if (shift > FAULT_EMAIL_MAX_BACKOFF_SHIFT) shift = FAULT_EMAIL_MAX_BACKOFF_SHIFT;
  return FAULT_EMAIL_RETRY_INTERVAL_MS << shift;
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

static void resetTowerFaultDetection(uint8_t tIdx, unsigned long now) {
  Tower &t = towers[tIdx];
  t.lastEdgeMs         = 0;
  t.prevEdgeMs         = 0;
  t.lastIntervalMs     = 0;
  t.traysSinceActive   = 0;
  t.inhibitUntilMs     = now + CHANGE_INHIBIT_MS;
  clearIntervalHistory(tIdx);
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
  t.faultEmailFails         = 0;
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
    // [FIX-29] irrigation count is left alone (owned by towerXIrrEnable)
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

  if (intermittentMode && !intermittentPhaseOn) return;
  if (!towerOnEnabled[i]) return;

  if (!t.contactorOn || t.fault || t.bypassLine || now <= t.inhibitUntilMs)
    return;

  if (t.traysSinceActive < LEARNING_TRAYS)
    return;

  unsigned long vfdInterval = t.interval_ms;
  unsigned long avgMeasured = rollingAvgInterval(i);

  unsigned long baseInterval;
  if (avgMeasured > 0 && t.intervalHistCnt >= INTERVAL_HISTORY_SIZE)
    baseInterval = (vfdInterval > avgMeasured) ? vfdInterval : avgMeasured;
  else
    baseInterval = vfdInterval;

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

unsigned long  inputActiveStartMs[NUM_CH];
bool           stuckSensorFault[NUM_CH];

// ======================================================================
// [FIX-17] TOWER 8 INDEPENDENT IRRIGATION PARAMETERS
//
// Tower 8 (array index 7) uses its own pulse duration and cycles/day.
//   tower8PulseSeconds      → pulse duration (default 5 s, read live)
//   tower8IrrCyclesPerDay   → daily target = cycles × 16 (default 48)
//
// Towers 1-7 keep using the global pulseSeconds / PULSE_ON_MS (40 s)
// and a single cycle of 16 trays.
//
// [FIX-17a] No cached PULSE_ON_MS for Tower 8 — pulseDurationForTower()
// reads tower8PulseSeconds from the cloud variable on every call. This
// guarantees the latest dashboard value is always used, with no
// dependence on the change callback firing in time.
// ======================================================================

static const uint8_t TOWER8_IDX        = NUM_TOWERS - 1;   // index 7 = "Tower 8"
static const uint8_t TRAYS_PER_CYCLE   = 16;

// [FIX-17a] Read cloud variable directly each call — no cached copy.
static inline unsigned long pulseDurationForTower(uint8_t ch) {
  if (ch == TOWER8_IDX) {
    long s = tower8PulseSeconds;
    if (s < 1)   s = 1;
    if (s > 600) s = 600;
    return (unsigned long)s * 1000UL;
  }
  return PULSE_ON_MS;
}

static inline uint16_t cyclesForTower(uint8_t ch) {
  if (ch == TOWER8_IDX) {
    int c = tower8IrrCyclesPerDay;
    if (c < 1)  c = 1;
    if (c > 10) c = 10;
    return (uint16_t)c;
  }
  return 1;
}

static inline uint16_t targetTraysForTower(uint8_t ch) {
  return cyclesForTower(ch) * TRAYS_PER_CYCLE;
}

// [FIX-17] Per-tower completion check — target = cycles × 16
static inline bool towerIrrigationComplete(uint8_t towerIdx) {
  if (towerIdx >= NUM_TOWERS) return false;
  return towerIrrCount[towerIdx] >= targetTraysForTower(towerIdx);
}

static inline bool anyTowerIrrEnabled() {
  for (uint8_t i = 0; i < NUM_TOWERS; ++i)
    if (towerIrrEnabled[i]) return true;
  return false;
}

static inline bool channelIrrigationEnabled(uint8_t ch) {
  if (ch < NUM_TOWERS) return towerIrrEnabled[ch];
  return anyTowerIrrEnabled();
}

static inline bool anyIrrigationValveOn() {
  for (uint8_t ch = 0; ch < NUM_CH; ++ch)
    if (outStateArr[ch]) return true;
  return false;
}

static void forceStopChannelIrrigation(DigitalMechExpansion& exp, uint8_t ch) {
  exp.digitalWrite(ch, LOW);
  outStateArr[ch] = 0; offAtMs[ch] = 0;
  firedThisAssert[ch] = false;
  ignoreUntilMs[ch] = millis() + IGNORE_MS;
}

static void forceStopAllIrrigation(DigitalMechExpansion& exp) {
  for (uint8_t ch = 0; ch < NUM_CH; ++ch)
    forceStopChannelIrrigation(exp, ch);
  exp.updateDigitalOutputs();
}

bool          wasCloudConnected = false;
unsigned long disconnectStartMs = 0;
unsigned long lastUptimeBeforeDisconnectSec = 0;
unsigned long lastDayIndex = 0;

inline void fireValve(DigitalMechExpansion& exp, uint8_t ch, unsigned long now) {
  if (ch >= NUM_CH) return;
  const bool isTowerChannel = (ch < NUM_TOWERS);

  if (isTowerChannel && !towerIrrEnabled[ch]) {
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

  // [FIX-17a] Compute duration once, use for both off-time and logging
  unsigned long duration = pulseDurationForTower(ch);

  exp.digitalWrite(ch, HIGH);
  outStateArr[ch] = 1;
  offAtMs[ch] = now + duration;
  firedThisAssert[ch] = true;
  ignoreUntilMs[ch] = now + IGNORE_MS;
  exp.updateDigitalOutputs();

  if (isTowerChannel) {
    towerIrrCount[ch]++;

    // [FIX-17a] Log the actual duration used so we can verify Tower 8 is honoring cloud value
    Serial.print(F("[IRR] T")); Serial.print(ch + 1);
    Serial.print(F(" valve open #")); Serial.print(towerIrrCount[ch]);
    Serial.print(F("/")); Serial.print(targetTraysForTower(ch));
    Serial.print(F(" for "));
    Serial.print(duration / 1000UL);
    Serial.println(F(" s"));

    if (towerIrrigationComplete(ch)) {
      Serial.print(F("[IRR] T")); Serial.print(ch + 1);
      Serial.print(F(" complete ("));
      Serial.print(targetTraysForTower(ch));
      Serial.println(F(" trays). Disabling tower irrigation."));
      towerIrrEnabled[ch] = false;

      switch (ch) {
        case 0: tower0IrrEnable = false; break;
        case 1: tower1IrrEnable = false; break;
        case 2: tower2IrrEnable = false; break;
        case 3: tower3IrrEnable = false; break;
        case 4: tower4IrrEnable = false; break;
        case 5: tower5IrrEnable = false; break;
        case 6: tower6IrrEnable = false; break;
        case 7: tower7IrrEnable = false; break;
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
  {
    char ub[24];
    fmtUptimeBuf((unsigned long)uptimeSec, ub, sizeof(ub));
    uptimeStr = ub;                                   // [FIX-9] no temp String
  }

  IPAddress ip = Ethernet.localIP();
  if (ip) {
    char buf[24];
    snprintf(buf, sizeof(buf), "%u.%u.%u.%u", ip[0], ip[1], ip[2], ip[3]);
    ipAddressStr = buf;
  } else ipAddressStr = "0.0.0.0";

  nextPublish = nowMs + 10000UL;
}

// Cloud callbacks
void onPulseSecondsChange() {
  if (pulseSeconds < 1)   pulseSeconds = 1;
  if (pulseSeconds > 600) pulseSeconds = 600;
  PULSE_ON_MS = (unsigned long)pulseSeconds * 1000UL;
  unsigned long now = millis();
  // [FIX-17] honor per-tower duration so changing global doesn't truncate Tower 8
  for (uint8_t ch = 0; ch < NUM_CH; ++ch)
    if (outStateArr[ch]) offAtMs[ch] = now + pulseDurationForTower(ch);
}

// [FIX-17a] Tower 8 pulse callback — clamps value and retargets active valve.
// No cached *_MS variable to update anymore (pulseDurationForTower reads live).
void onTower8PulseSecondsChange() {
  if (tower8PulseSeconds < 1)   tower8PulseSeconds = 1;
  if (tower8PulseSeconds > 600) tower8PulseSeconds = 600;
  unsigned long now = millis();
  // If Tower 8 valve is currently open, retarget its off time to the new duration
  if (outStateArr[TOWER8_IDX])
    offAtMs[TOWER8_IDX] = now + (unsigned long)tower8PulseSeconds * 1000UL;
  Serial.print(F("[IRR] Tower 8 pulse changed → "));
  Serial.print(tower8PulseSeconds);
  Serial.println(F(" s"));
}

// [FIX-17] Tower 8 cycles-per-day callback
void onTower8IrrCyclesPerDayChange() {
  if (tower8IrrCyclesPerDay < 1)  tower8IrrCyclesPerDay = 1;
  if (tower8IrrCyclesPerDay > 10) tower8IrrCyclesPerDay = 10;
  Serial.print(F("[IRR] Tower 8 cycles/day = "));
  Serial.print(tower8IrrCyclesPerDay);
  Serial.print(F(" (target = "));
  Serial.print(tower8IrrCyclesPerDay * TRAYS_PER_CYCLE);
  Serial.println(F(" trays)"));
}

static void handleTowerIrrEnableChange(uint8_t tIdx, bool enabled) {
  if (tIdx >= NUM_TOWERS) return;

  bool wasEnabled = towerIrrEnabled[tIdx];
  towerIrrEnabled[tIdx] = enabled;

  DigitalMechExpansion exp1 = OptaController.getExpansion(IRR_EXP_IDX);

  if (!enabled) {
    if (outStateArr[tIdx]) {
      forceStopChannelIrrigation(exp1, tIdx);
      exp1.updateDigitalOutputs();
    }
    Serial.print(F("[IRR] Tower ")); Serial.print(tIdx + 1);
    Serial.println(F(" irrigation DISABLED."));
    return;
  }

  if (!wasEnabled) {
    unsigned long now = millis();
    towerIrrCount[tIdx] = 0;
    trigCountArr[tIdx] = 0;
    lastTrigMsArr[tIdx] = 0;
    prevTrigMsArr[tIdx] = 0;
    deltaSecArr[tIdx] = 0;

    PinStatus level = exp1.digitalRead(tIdx);
    lastRaw[tIdx] = level;
    lastStable[tIdx] = level;
    lastChangeMsIrr[tIdx] = now;

    firedThisAssert[tIdx] = (level == ACTIVE_LEVEL);

    ignoreUntilMs[tIdx] = 0;
    offAtMs[tIdx] = 0;
    outStateArr[tIdx] = 0;
    inputActiveStartMs[tIdx] = 0;
    stuckSensorFault[tIdx] = false;

    Serial.print(F("[IRR] Tower ")); Serial.print(tIdx + 1);
    Serial.println(F(" irrigation ENABLED. Counting from next beep."));
  }
}

void onTower0IrrEnableChange() { handleTowerIrrEnableChange(0, tower0IrrEnable); }
void onTower1IrrEnableChange() { handleTowerIrrEnableChange(1, tower1IrrEnable); }
void onTower2IrrEnableChange() { handleTowerIrrEnableChange(2, tower2IrrEnable); }
void onTower3IrrEnableChange() { handleTowerIrrEnableChange(3, tower3IrrEnable); }
void onTower4IrrEnableChange() { handleTowerIrrEnableChange(4, tower4IrrEnable); }
void onTower5IrrEnableChange() { handleTowerIrrEnableChange(5, tower5IrrEnable); }
void onTower6IrrEnableChange() { handleTowerIrrEnableChange(6, tower6IrrEnable); }
void onTower7IrrEnableChange() { handleTowerIrrEnableChange(7, tower7IrrEnable); }

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

  const VfdExtTelemetry &e1 = vfdGroups[1].ext;
  vfd1MotorCurrentPct  = e1.motorCurrentPct;
  vfd1MotorCurrentA    = e1.motorCurrentA;      // [FIX-28]
  vfd1MotorTorquePct   = e1.motorTorquePct;     // [FIX-28]
  vfd1PeakCurrentPct   = e1.peakCurrentPct;
  vfd1AccelTimeSec     = e1.accelTimeSec;
  vfd1DecelTimeSec     = e1.decelTimeSec;
  vfd1CurrentLimitPct  = e1.currentLimitPct;

  const VfdExtTelemetry &e2 = vfdGroups[0].ext;
  vfd2MotorCurrentPct  = e2.motorCurrentPct;
  vfd2MotorCurrentA    = e2.motorCurrentA;      // [FIX-28]
  vfd2MotorTorquePct   = e2.motorTorquePct;     // [FIX-28]
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

static void handleTowerOnChange(uint8_t tIdx, bool on) {
  if (tIdx >= NUM_TOWERS) return;
  towerOnEnabled[tIdx] = on;

  Serial.print(F("[TOWER-ON] Tower "));
  Serial.print(tIdx + 1);
  Serial.println(on ? " ON" : " OFF");

  if (on) {
    towers[tIdx].fault = false;
    resetTowerFaultDetection(tIdx, millis());
    towers[tIdx].inhibitUntilMs = millis() + CHANGE_INHIBIT_MS;
  } else {
    if (!towers[tIdx].bypassLine) {
      towers[tIdx].desiredContactorOn = false;
      requestContactorChange(tIdx, false);
    }
  }
}

void onTower0OnChange() { handleTowerOnChange(0, tower0On); }
void onTower1OnChange() { handleTowerOnChange(1, tower1On); }
void onTower2OnChange() { handleTowerOnChange(2, tower2On); }
void onTower3OnChange() { handleTowerOnChange(3, tower3On); }
void onTower4OnChange() { handleTowerOnChange(4, tower4On); }
void onTower5OnChange() { handleTowerOnChange(5, tower5On); }
void onTower6OnChange() { handleTowerOnChange(6, tower6On); }
void onTower7OnChange() { handleTowerOnChange(7, tower7On); }

// ======================================================================
// SECTION 4: VFD MODBUS/RS485  (SEW MOVITRAC LTE-B+)
// ======================================================================

const uint8_t VFD1_ID = 1;
const uint8_t VFD2_ID = 2;

const uint16_t REG_CMD_WORD     = 0;
const uint16_t REG_SPEED_REF    = 1;
const uint16_t REG_STATUS_WORD  = 5;
const uint16_t REG_ACTUAL_SPEED = 6;
const uint16_t REG_ACTUAL_CURR  = 7;
const uint16_t REG_MOTOR_TORQUE = 8;

const uint16_t REG_ACCEL_TIME      = 130;
const uint16_t REG_DECEL_TIME      = 131;
const uint16_t REG_RATED_CURRENT   = 135;   // [FIX-28] P-08, manual reg 136
const uint16_t REG_CURRENT_LIMIT   = 181;

// [FIX-28] P-08 raw value x 0.1 = amps (drive shows it with 1 decimal).
// If the dashboard amps look wrong, compare the "[VFD-EXT] ... P-08 raw"
// serial line with P-08 on the drive keypad, or set the motor nameplate
// current here (0 = use P-08 read from the drive).
static const float RATED_CURRENT_SCALE          = 0.1f;
static const float VFD1_RATED_CURRENT_A_OVERRIDE = 0.0f;
static const float VFD2_RATED_CURRENT_A_OVERRIDE = 0.0f;

const float VFD_MAX_FREQ_HZ = 53.3f;
const float VFD_SCALE_FULL  = 16384.0f;

static const unsigned long VFD_POLL_INTERVAL_MS = 500;
static unsigned long nextVfdPollMs = 0;
static unsigned long modbusInterFrameUs = 0;

inline uint16_t vfdEncodeSpeed(float hz) {
  if (hz < 0.0f) hz = 0.0f;
  if (hz > VFD_MAX_FREQ_HZ) hz = VFD_MAX_FREQ_HZ;
  return (uint16_t)round((hz / VFD_MAX_FREQ_HZ) * VFD_SCALE_FULL);
}

inline float vfdDecodeSpeed(uint16_t raw) {
  return ((float)raw / VFD_SCALE_FULL) * VFD_MAX_FREQ_HZ;
}

inline float vfdDecodeCurrent(uint16_t raw) {
  return ((float)raw / VFD_SCALE_FULL) * 100.0f;
}

static bool vfdWriteCommand(uint8_t slaveId, bool run, float speedHz) {
  uint16_t cmd   = run ? PO1_RUN : PO1_STOP_RAPID;
  uint16_t speed = vfdEncodeSpeed(speedHz);
  if (!ModbusRTUClient.holdingRegisterWrite(slaveId, REG_SPEED_REF, speed))
    return false;
  delayMicroseconds(modbusInterFrameUs);
  if (!ModbusRTUClient.holdingRegisterWrite(slaveId, REG_CMD_WORD, cmd))
    return false;
  return true;
}

// [FIX-18] Rising edge on PO1 bit 6 resets a drive fault. We write
// 0x0040 now; the next RUN/STOP write clears the bit, so every reset is
// a clean 0->1 edge. The motor stays stopped until RUN is re-sent.
static bool vfdPulseFaultReset(uint8_t slaveId) {
  return ModbusRTUClient.holdingRegisterWrite(slaveId, REG_CMD_WORD, PO1_FAULT_RESET);
}

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

  alarmCode = (rawStatus & 0x0020) ? (int16_t)((rawStatus >> 8) & 0xFF) : 0;
  return true;
}

// [FIX-28] PI3 (current) and PI4 (torque) in one transaction. Torque is
// signed (negative while the motor is braking) and uses the same
// 0x4000 = 100 % scaling as current.
static bool vfdReadCurrentTorque(uint8_t slaveId, float &currentPct,
                                 float &torquePct) {
  if (!ModbusRTUClient.requestFrom(slaveId, HOLDING_REGISTERS,
                                   REG_ACTUAL_CURR, 2))
    return false;
  if (ModbusRTUClient.available() < 2) return false;
  uint16_t rawCurr   = (uint16_t)ModbusRTUClient.read();
  int16_t  rawTorque = (int16_t)(uint16_t)ModbusRTUClient.read();
  currentPct = vfdDecodeCurrent(rawCurr);
  torquePct  = ((float)rawTorque / VFD_SCALE_FULL) * 100.0f;
  return true;
}

static bool vfdReadExtendedParams(uint8_t slaveId, VfdExtTelemetry &ext) {
  bool allOk = true;

  if (ModbusRTUClient.requestFrom(slaveId, HOLDING_REGISTERS,
                                   REG_ACCEL_TIME, 2)) {
    if (ModbusRTUClient.available() >= 2) {
      uint16_t rawAccel = (uint16_t)ModbusRTUClient.read();
      uint16_t rawDecel = (uint16_t)ModbusRTUClient.read();
      ext.accelTimeSec = (float)rawAccel * 0.01f;
      ext.decelTimeSec = (float)rawDecel * 0.01f;
    } else { allOk = false; }
  } else { allOk = false; }

  delayMicroseconds(modbusInterFrameUs);

  {
    long val = ModbusRTUClient.holdingRegisterRead(slaveId, REG_CURRENT_LIMIT);
    if (!ModbusRTUClient.lastError()) {
      ext.currentLimitPct = (float)val * 0.1f;
    } else { allOk = false; }
  }

  // [FIX-28] rated motor current for the % -> amps conversion
  float ovr = (slaveId == VFD1_ID) ? VFD1_RATED_CURRENT_A_OVERRIDE
                                   : VFD2_RATED_CURRENT_A_OVERRIDE;
  if (ovr > 0.0f) {
    ext.ratedCurrentA = ovr;
  } else {
    delayMicroseconds(modbusInterFrameUs);
    long val = ModbusRTUClient.holdingRegisterRead(slaveId, REG_RATED_CURRENT);
    if (!ModbusRTUClient.lastError()) {
      ext.ratedCurrentA = (float)val * RATED_CURRENT_SCALE;
      Serial.print(F("[VFD-EXT] Slave ")); Serial.print(slaveId);
      Serial.print(F(" P-08 raw=")); Serial.print(val);
      Serial.print(F(" -> ")); Serial.print(ext.ratedCurrentA, 1);
      Serial.println(F(" A"));
    } else { allOk = false; }
  }

  ext.extReadOk = allOk;
  if (!allOk) ext.modbusExtErrCount++;

  return allOk;
}

static bool vfdWriteAccelTime(uint8_t slaveId, float seconds) {
  if (seconds < 0.0f)   seconds = 0.0f;
  if (seconds > 600.0f) seconds = 600.0f;
  uint16_t raw = (uint16_t)round(seconds / 0.01f);

  Serial.print(F("[VFD-WRITE] Slave "));
  Serial.print(slaveId);
  Serial.print(F(" P-03 accel = "));
  Serial.print(seconds, 2);
  Serial.print(F(" s (raw="));
  Serial.print(raw);
  Serial.println(F(")"));

  bool ok = ModbusRTUClient.holdingRegisterWrite(slaveId, REG_ACCEL_TIME, raw);
  if (!ok) {
    Serial.print(F("[VFD-WRITE] FAILED slave "));
    Serial.println(slaveId);
  }
  return ok;
}

static void updateTowerIntervalFromVfdFeedback() {
  bool have1 = (!vfdGroups[1].commFault && vfdGroups[1].lastFeedbackHz > 1.0f);
  bool have2 = (!vfdGroups[0].commFault && vfdGroups[0].lastFeedbackHz > 1.0f);
  if (!have1 && !have2) return;

  unsigned long nowMs = millis();

  for (uint8_t g = 0; g < NUM_VFD_GROUPS; ++g) {
    VfdGroup &vg = vfdGroups[g];
    if (vg.prevFeedbackHz > 1.0f && vg.lastFeedbackHz > 1.0f) {
      float delta = vg.lastFeedbackHz - vg.prevFeedbackHz;
      if (delta < 0) delta = -delta;
      if (delta >= SPEED_CHANGE_THRESHOLD_HZ) {
        unsigned long newInterval = computeTowerIntervalMsFromHz(vg.lastFeedbackHz);
        unsigned long grace = newInterval * 2;

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
          towers[i].traysSinceActive = 0;
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
// [FIX-18] Cloud run switches are an operator override. The immediate
// write is a courtesy; if it fails, reconciliation retries on the next
// poll, so commFault is no longer set here (a lost write is not a comm
// fault, and flagging it hid the real state).
void onVfd1RunCmdChange() {
  Serial.print(F("[CLOUD] vfd1RunCmd = ")); Serial.println(vfd1RunCmd ? "ON" : "OFF");
  vfdGroups[1].runCmd = vfd1RunCmd;
  if (vfdGroups[1].state == VGS_RUNNING)
    vfdWriteCommand(VFD1_ID, vfdWantsRun(vfdGroups[1]), vfdGroups[1].speedSetHz);
}
void onVfd2RunCmdChange() {
  Serial.print(F("[CLOUD] vfd2RunCmd = ")); Serial.println(vfd2RunCmd ? "ON" : "OFF");
  vfdGroups[0].runCmd = vfd2RunCmd;
  if (vfdGroups[0].state == VGS_RUNNING)
    vfdWriteCommand(VFD2_ID, vfdWantsRun(vfdGroups[0]), vfdGroups[0].speedSetHz);
}
void onVfd2AlarmCodeChange() {}

void onIntermittentModeChange() {
  Serial.print(F("[INTERMITTENT] Mode "));
  Serial.println(intermittentMode ? "ENABLED" : "DISABLED");

  unsigned long nowMs = millis();

  if (intermittentMode) {
    intermittentPhaseOn      = true;
    intermittentPhaseStartMs = nowMs;
  } else {
    if (!intermittentPhaseOn) {
      Serial.println(F("[INTERMITTENT] Resuming VFDs (mode disabled during OFF phase)"));
      for (uint8_t g = 0; g < NUM_VFD_GROUPS; ++g) {
        VfdGroup &vg = vfdGroups[g];
        if (vg.state == VGS_RUNNING && vg.runCmd) {
          vfdWriteCommand(vg.slaveId, true, vg.speedSetHz);
        }
      }
      for (uint8_t i = 0; i < NUM_TOWERS; ++i) {
        resetTowerFaultDetection(i, nowMs);
        towers[i].inhibitUntilMs = nowMs + INTERMITTENT_RESTART_GRACE_MS;
      }
    }
    intermittentPhaseOn = true;
  }
}

static void intermittentStopVfds(unsigned long nowMs) {
  Serial.println(F("[INTERMITTENT] OFF phase — stopping VFDs"));
  for (uint8_t g = 0; g < NUM_VFD_GROUPS; ++g) {
    VfdGroup &vg = vfdGroups[g];
    if (vg.state == VGS_RUNNING) {
      bool ok = vfdWriteCommand(vg.slaveId, false, vg.speedSetHz);
      vg.commFault = !ok;
    }
  }
}

static void intermittentStartVfds(unsigned long nowMs) {
  Serial.println(F("[INTERMITTENT] ON phase — starting VFDs"));
  for (uint8_t g = 0; g < NUM_VFD_GROUPS; ++g) {
    VfdGroup &vg = vfdGroups[g];
    if (vg.state == VGS_RUNNING && vg.runCmd) {
      bool ok = vfdWriteCommand(vg.slaveId, true, vg.speedSetHz);
      vg.commFault = !ok;
    }
  }
  for (uint8_t i = 0; i < NUM_TOWERS; ++i) {
    resetTowerFaultDetection(i, nowMs);
    towers[i].inhibitUntilMs = nowMs + INTERMITTENT_RESTART_GRACE_MS;
  }
}

static void processIntermittentMode(unsigned long nowMs) {
  if (!intermittentMode) return;

  unsigned long elapsed = nowMs - intermittentPhaseStartMs;

  if (intermittentPhaseOn) {
    if (elapsed >= INTERMITTENT_ON_MS) {
      intermittentPhaseOn      = false;
      intermittentPhaseStartMs = nowMs;
      intermittentStopVfds(nowMs);
    }
  } else {
    if (elapsed >= INTERMITTENT_OFF_MS) {
      intermittentPhaseOn      = true;
      intermittentPhaseStartMs = nowMs;
      intermittentStartVfds(nowMs);
    }
  }
}

void onVfd1AccelTimeSecChange() {
  float sec = vfd1AccelTimeSec;
  if (sec < 0.0f)   sec = 0.0f;
  if (sec > 600.0f) sec = 600.0f;
  vfd1AccelTimeSec = sec;

  Serial.print(F("[CLOUD] VFD1 accel time set to "));
  Serial.print(sec, 2);
  Serial.println(F(" s"));

  bool ok = vfdWriteAccelTime(VFD1_ID, sec);
  if (ok) {
    vfdGroups[1].ext.accelTimeSec = sec;
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

// ======================================================================
// [FIX-18] RUN-STATE RECONCILIATION
//
// Called after every successful PI1 status read while the group is in
// VGS_RUNNING. Makes the drive match what the controller wants, so the
// plant recovers on its own from:
//   - a boot with no cloud (run word never arrived from a callback)
//   - a drive stopped by its own P-36 comm timeout
//   - a drive power-cycled independently of the Opta
//   - a single failed Modbus write
//   - a resettable drive fault (reset pulsed, max once per 30 s)
// ======================================================================
static void reconcileVfdRunState(uint8_t g, VfdGroup &vg, uint16_t st,
                                 unsigned long nowMs) {
  vg.driveEnabled = (st & PI1_OUTPUT_ENABLED) != 0;
  vg.driveFault   = (st & PI1_FAULT) != 0;

  bool wantRun = vfdWantsRun(vg);

  // 1. Fault present and we want to run: pulse reset, then let the next
  //    poll re-send RUN. Never fight a fault faster than every 30 s.
  if (vg.driveFault) {
    if (wantRun && (nowMs - vg.lastFaultResetMs) >= VFD_FAULT_RESET_INTERVAL_MS) {
      vg.lastFaultResetMs = nowMs;
      vg.faultResetCount++;
      Serial.print(F("[VFD-RECONCILE] Group ")); Serial.print(g);
      Serial.print(F(" slave ")); Serial.print(vg.slaveId);
      Serial.print(F(" FAULT (PI1=0x")); Serial.print(st, HEX);
      Serial.print(F(") -> reset pulse #")); Serial.println(vg.faultResetCount);
      delayMicroseconds(modbusInterFrameUs);
      if (!vfdPulseFaultReset(vg.slaveId)) vg.commFault = true;
    }
    return;
  }

  // 2. Want RUN but output stage is off (display shows "StoP").
  if (wantRun && !vg.driveEnabled) {
    if ((nowMs - vg.lastReassertMs) >= VFD_REASSERT_INTERVAL_MS) {
      vg.lastReassertMs = nowMs;
      vg.reassertCount++;
      Serial.print(F("[VFD-RECONCILE] Group ")); Serial.print(g);
      Serial.print(F(" slave ")); Serial.print(vg.slaveId);
      Serial.print(F(" not enabled (PI1=0x")); Serial.print(st, HEX);
      Serial.print(F(") -> re-sending RUN @ ")); Serial.print(vg.speedSetHz, 1);
      Serial.print(F(" Hz, #")); Serial.println(vg.reassertCount);
      delayMicroseconds(modbusInterFrameUs);
      bool ok = vfdWriteCommand(vg.slaveId, true, vg.speedSetHz);
      if (!ok) vg.commFault = true;
    }
    return;
  }

  // 3. Want STOP but the drive is still enabled (e.g. intermittent OFF
  //    phase write was lost, or operator stop from cloud failed).
  if (!wantRun && vg.driveEnabled) {
    if ((nowMs - vg.lastReassertMs) >= VFD_REASSERT_INTERVAL_MS) {
      vg.lastReassertMs = nowMs;
      Serial.print(F("[VFD-RECONCILE] Group ")); Serial.print(g);
      Serial.print(F(" slave ")); Serial.print(vg.slaveId);
      Serial.println(F(" running but STOP wanted -> re-sending STOP"));
      delayMicroseconds(modbusInterFrameUs);
      bool ok = vfdWriteCommand(vg.slaveId, false, vg.speedSetHz);
      if (!ok) vg.commFault = true;
    }
  }
}

static void pollVfdsIfDue(unsigned long nowMs) {
  if ((long)(nowMs - nextVfdPollMs) >= 0) {

    for (uint8_t g = 0; g < NUM_VFD_GROUPS; ++g) {
      VfdGroup &vg = vfdGroups[g];
      if (vg.state != VGS_RUNNING) continue;

      // [FIX-19] a silent drive is retried every VFD_COMM_RETRY_MS, not
      // every poll, so two timeouts per 500 ms cannot starve the loop.
      if (vg.commFault && !timeReached(vg.nextCommRetryMs, nowMs)) continue;

      int16_t st = 0, al = 0;
      float sp = 0.0f;
      bool ok = vfdReadStatus(vg.slaveId, st, sp, al);
      if (!ok) {
        if (!vg.commFault) {
          Serial.print(F("[VFD] Group ")); Serial.print(g);
          Serial.print(F(" slave ")); Serial.print(vg.slaveId);
          Serial.println(F(" no response, backing off"));
        }
        vg.commFault      = true;
        vg.nextCommRetryMs = nowMs + VFD_COMM_RETRY_MS;
      } else {
        if (vg.commFault) {
          Serial.print(F("[VFD] Group ")); Serial.print(g);
          Serial.println(F(" comm restored"));
        }
        vg.commFault      = false;
        vg.lastFeedbackHz = sp;
      }

      delayMicroseconds(modbusInterFrameUs);

      float curPct = 0.0f, torqPct = 0.0f;
      bool curOk = ok && vfdReadCurrentTorque(vg.slaveId, curPct, torqPct);
      if (curOk) {
        vg.ext.motorCurrentPct = curPct;
        vg.ext.motorTorquePct  = torqPct;
        vg.ext.motorCurrentA   = curPct * 0.01f * vg.ext.ratedCurrentA;  // [FIX-28]
        if (curPct > vg.ext.peakCurrentPct)
          vg.ext.peakCurrentPct = curPct;
      }

      // [FIX-18] make the drive match what we want
      if (ok) reconcileVfdRunState(g, vg, (uint16_t)st, nowMs);

      if (g == 0) {
        vfd2CommFault  = vg.commFault;
        if (ok) { vfd2StatusWord = (int)st; vfd2SpeedFbHz = sp; vfd2AlarmCode = (int)al; }
      } else {
        vfd1CommFault  = vg.commFault;
        if (ok) { vfd1StatusWord = (int)st; vfd1SpeedFbHz = sp; vfd1AlarmCode = (int)al; }
      }

      delayMicroseconds(modbusInterFrameUs);
    }

    updateTowerIntervalFromVfdFeedback();
    nextVfdPollMs = nowMs + VFD_POLL_INTERVAL_MS;
  }

  if ((long)(nowMs - nextVfdExtPollMs) >= 0) {

    for (uint8_t g = 0; g < NUM_VFD_GROUPS; ++g) {
      VfdGroup &vg = vfdGroups[g];
      if (vg.state != VGS_RUNNING) continue;
      if (vg.commFault) continue;                 // [FIX-19] fast poll will clear it

      bool ok = vfdReadExtendedParams(vg.slaveId, vg.ext);

      if (ok) {
        Serial.print(F("[VFD-EXT] Slave "));
        Serial.print(vg.slaveId);
        Serial.print(F(": I=")); Serial.print(vg.ext.motorCurrentPct, 1);
        Serial.print(F("% (")); Serial.print(vg.ext.motorCurrentA, 2);
        Serial.print(F(" A) T=")); Serial.print(vg.ext.motorTorquePct, 1);
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
  Serial.println(F("OPTA combined v4.3: Towers + Irrigation + SEW MOVITRAC LTE-B+ VFDs"));
  Serial.println(F("[FIX-18] Offline-resilient: VFD run state reconciled every poll"));

  // Cloud variable defaults
  ipAddressStr = String(""); lastOutageType = String("none"); uptimeStr = String("");
#if !ENABLE_GRODAN
  grodanStatus = String("Disabled in firmware (FIX-27)");
#endif
  vfd1SpeedFbHz = vfd1SpeedSetHz = vfd2SpeedFbHz = vfd2SpeedSetHz = 0.0f;
  deltaMin0=deltaMin1=deltaMin2=deltaMin3=deltaMin4=deltaMin5=deltaMin6=deltaMin7=0;
  lastOfflineDurationSec=0; lastReconnectUptimeSec=0;
  lastTrigAgoMin0=lastTrigAgoMin1=lastTrigAgoMin2=lastTrigAgoMin3=
  lastTrigAgoMin4=lastTrigAgoMin5=lastTrigAgoMin6=lastTrigAgoMin7=-1;
  networkLossCount=0; powerLossCount=0; pulseSeconds=40;

  // [FIX-17] Tower 8 irrigation defaults
  tower8PulseSeconds    = 5;
  tower8IrrCyclesPerDay = 3;

  // [FIX-14] Per-tower irrigation defaults: all OFF
  tower0IrrEnable=false; tower1IrrEnable=false;
  tower2IrrEnable=false; tower3IrrEnable=false;
  tower4IrrEnable=false; tower5IrrEnable=false;
  tower6IrrEnable=false; tower7IrrEnable=false;
  for (uint8_t i = 0; i < NUM_TOWERS; ++i) towerIrrEnabled[i] = false;

  // [FIX-16] Per-tower ON/OFF defaults: all ON
  tower0On=tower1On=tower2On=tower3On=
  tower4On=tower5On=tower6On=tower7On=true;
  for (uint8_t i = 0; i < NUM_TOWERS; ++i) towerOnEnabled[i] = true;

  tower0LastEdgeMin=tower0LastIntervalMin=tower1LastEdgeMin=tower1LastIntervalMin=
  tower2LastEdgeMin=tower2LastIntervalMin=tower3LastEdgeMin=tower3LastIntervalMin=
  tower4LastEdgeMin=tower4LastIntervalMin=tower5LastEdgeMin=tower5LastIntervalMin=
  tower6LastEdgeMin=tower6LastIntervalMin=tower7LastEdgeMin=tower7LastIntervalMin=-1;
  trigCount0=trigCount1=trigCount2=trigCount3=trigCount4=trigCount5=trigCount6=trigCount7=0;
  uptimeSec=0;
  vfd1AlarmCode=vfd1StatusWord=vfd2AlarmCode=vfd2StatusWord=0;
  inState0=inState1=inState2=inState3=inState4=inState5=inState6=inState7=false;
  outState0=outState1=outState2=outState3=outState4=outState5=outState6=outState7=false;
  tower0Bypass=tower1Bypass=tower2Bypass=tower3Bypass=
  tower4Bypass=tower5Bypass=tower6Bypass=tower7Bypass=false;
  tower0Fault=tower1Fault=tower2Fault=tower3Fault=
  tower4Fault=tower5Fault=tower6Fault=tower7Fault=false;
  tower0Running=tower1Running=tower2Running=tower3Running=
  tower4Running=tower5Running=tower6Running=tower7Running=false;
  vfd1CommFault=vfd2CommFault=false;
  vfd1RunCmd=vfd2RunCmd=RUN_VFDS_AT_BOOT;        // [FIX-18] run without cloud
  intermittentMode=false;
  tower0TrayCount=tower1TrayCount=tower2TrayCount=tower3TrayCount=
  tower4TrayCount=tower5TrayCount=tower6TrayCount=tower7TrayCount=0;

  vfd1MotorCurrentPct=vfd2MotorCurrentPct=0.0f;
  vfd1MotorCurrentA=vfd2MotorCurrentA=0.0f;        // [FIX-28]
  vfd1MotorTorquePct=vfd2MotorTorquePct=0.0f;      // [FIX-28]
  vfd1PeakCurrentPct=vfd2PeakCurrentPct=0.0f;
  vfd1AccelTimeSec=vfd2AccelTimeSec=0.0f;
  vfd1DecelTimeSec=vfd2DecelTimeSec=0.0f;
  vfd1CurrentLimitPct=vfd2CurrentLimitPct=0.0f;

  // [FIX-24] Our own property registration and static-IP handler; the
  // generated initProperties()/ArduinoIoTPreferredConnection are NOT used.
  initPropertiesStatic();

  // [FIX-20] Shorten the handler's blocking Ethernet.begin() (15 s -> 8 s)
  // and stretch its INIT retry (0.5 s -> 20 s). Without this, a network
  // outage freezes loop() ~97% of the time and the drives time out.
  // [FIX-25] WORKAROUND for Arduino_ConnectionHandler static-IP bug.
  // EthernetConnectionHandler's (ip,dns,gw,mask) constructor stores an IPv4
  // address in dword[3] (bytes 12..15) of models::ip_addr, but
  // update_handleInit() rebuilds it with IPAddress(type, bytes), which reads
  // bytes 0..3. Those are zero, so the handler sees INADDR_NONE and silently
  // falls back to DHCP ("Waiting Ethernet configuration from DHCP server").
  // We rewrite the settings so the address sits where the handler reads it.
  // This block is REQUIRED for the static address to take effect.
  {
    models::NetworkSetting ns;
    StaticEthConnection.getSetting(ns);

    auto putV4 = [](models::ip_addr &dst, const IPAddress &src) {
      memset(dst.bytes, 0, sizeof(dst.bytes));
      dst.type     = IPv4;
      dst.dword[0] = (uint32_t)src;      // bytes 0..3, where IPAddress(type, bytes) reads
    };
#if USE_STATIC_IP
    putV4(ns.eth.ip,      OPTA_STATIC_IP);
    putV4(ns.eth.dns,     OPTA_DNS);
    putV4(ns.eth.gateway, OPTA_GATEWAY);
    putV4(ns.eth.netmask, OPTA_NETMASK);
#else
    // [FIX-26] DHCP: an all-zero ip makes update_handleInit() take the DHCP
    // branch. dns/gw/mask come from the lease.
    memset(ns.eth.ip.bytes,      0, sizeof(ns.eth.ip.bytes));      ns.eth.ip.type      = IPv4;
    memset(ns.eth.dns.bytes,     0, sizeof(ns.eth.dns.bytes));     ns.eth.dns.type     = IPv4;
    memset(ns.eth.gateway.bytes, 0, sizeof(ns.eth.gateway.bytes)); ns.eth.gateway.type = IPv4;
    memset(ns.eth.netmask.bytes, 0, sizeof(ns.eth.netmask.bytes)); ns.eth.netmask.type = IPv4;
#endif

    ns.eth.timeout          = ETH_BEGIN_TIMEOUT_MS;      // [FIX-20]
    ns.eth.response_timeout = ETH_RESPONSE_TIMEOUT_MS;
    bool okSet = StaticEthConnection.updateSetting(ns);
    StaticEthConnection.updateTimeoutInterval(
        NetworkConnectionState::INIT, ETH_INIT_RETRY_MS);

    // read back and prove what the handler will actually use
    models::NetworkSetting chk;
    StaticEthConnection.getSetting(chk);
    IPAddress effective(chk.eth.ip.type, chk.eth.ip.bytes);
    Serial.print(F("[FIX-25/26] Network mode: "));
    Serial.print(USE_STATIC_IP ? F("STATIC") : F("DHCP"));
    Serial.print(F(", updateSetting ")); Serial.print(okSet ? "OK" : "REFUSED");
    Serial.print(F(", handler ip field = ")); Serial.println(effective);
  }
  Serial.println(F("[FIX-24] NetworkConfigurator bypassed"));

  // ArduinoCloud.begin() also arms the 32.7 s IWDG on the Opta [FIX-22].
  // Ethernet is initialised by the handler inside ArduinoCloud.update().
  ArduinoCloud.begin(StaticEthConnection);
  ArduinoCloud.setOTAChunkMode();

  PULSE_ON_MS = (unsigned long)pulseSeconds * 1000UL;

  lastDayIndex = dayIndexNow();
  lastOutageType = String("none");
  lastOfflineDurationSec=0; powerLossCount=0; networkLossCount=0;
  lastReconnectUptimeSec=0;

  vfdGroups[0].slaveId      = VFD2_ID;
  vfdGroups[0].state        = VGS_RUNNING;
  vfdGroups[0].speedSetHz   = FIXED_TOWER_SPEED_HZ;
  vfdGroups[0].runCmd       = RUN_VFDS_AT_BOOT;   // [FIX-18]
  vfdGroups[0].commFault    = false;
  vfdGroups[0].lastFeedbackHz = 0.0f;
  vfdGroups[0].prevFeedbackHz = 0.0f;

  vfdGroups[1].slaveId      = VFD1_ID;
  vfdGroups[1].state        = VGS_RUNNING;
  vfdGroups[1].speedSetHz   = FIXED_TOWER_SPEED_HZ;
  vfdGroups[1].runCmd       = RUN_VFDS_AT_BOOT;   // [FIX-18]
  vfdGroups[1].commFault    = false;
  vfdGroups[1].lastFeedbackHz = 0.0f;
  vfdGroups[1].prevFeedbackHz = 0.0f;

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

  for (uint8_t e = 0; e < NUM_EXPANSIONS; ++e)
    safeUpdateDigitalInputs(e);

  for (uint8_t i = 0; i < NUM_TOWERS; i++) {
    DigitalMechExpansion ex = OptaController.getExpansion(REED_CH[i].exp);
    PinStatus level = ex.digitalRead(REED_CH[i].ch);
    towers[i].rawReed = level; towers[i].stableReed = level;
  }

  for (uint8_t i = 0; i < NUM_TOWERS; i++)
    setRelayChan(towers[i].out, false);

  tower0Bypass=towers[0].bypassLine; tower1Bypass=towers[1].bypassLine;
  tower2Bypass=towers[2].bypassLine; tower3Bypass=towers[3].bypassLine;
  tower4Bypass=towers[4].bypassLine; tower5Bypass=towers[5].bypassLine;
  tower6Bypass=towers[6].bypassLine; tower7Bypass=towers[7].bypassLine;

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
  }

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
      ModbusRTUClient.setTimeout(VFD_MODBUS_TIMEOUT_MS);   // [FIX-19]
      Serial.print(F("Modbus RTU Client started, timeout "));
      Serial.print(VFD_MODBUS_TIMEOUT_MS); Serial.println(F(" ms"));
    }

    vfd1SpeedSetHz = FIXED_TOWER_SPEED_HZ;
    vfd2SpeedSetHz = FIXED_TOWER_SPEED_HZ;
    vfd1RunCmd = vfd2RunCmd = RUN_VFDS_AT_BOOT;           // [FIX-18]
  }

  // [FIX-20] No sketch-side Ethernet.begin(). The cloud connection handler
  // owns the interface and brings it up (DHCP or the static address from
  // arduino_secrets.h) inside ArduinoCloud.update(). The IP is reported
  // in ipAddressStr once it is up.

  Serial.println(F("Setup complete."));
  Serial.print(F("[FIX-18] VFDs at boot: "));
  Serial.println(RUN_VFDS_AT_BOOT ? F("RUN (cloud not required)") : F("STOPPED"));
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
  Serial.println(F("[FIX-14] Per-tower irrigation (default: all OFF)."));
  Serial.print(F("[FIX-15] Intermittent mode: OFF ("));
  Serial.print(INTERMITTENT_ON_MS / 60000UL);
  Serial.print(F(" min ON / "));
  Serial.print(INTERMITTENT_OFF_MS / 60000UL);
  Serial.println(F(" min OFF when enabled)."));
  Serial.println(F("[FIX-16] Per-tower ON/OFF switches (default: all ON)."));

  // [FIX-17a] Tower 8 irrigation summary — confirms which value the firmware booted with
  Serial.print(F("[FIX-17a] Tower 8 pulse = "));
  Serial.print(tower8PulseSeconds);
  Serial.print(F(" s (read live each firing), cycles/day = "));
  Serial.print(tower8IrrCyclesPerDay);
  Serial.print(F(" (target = "));
  Serial.print(tower8IrrCyclesPerDay * TRAYS_PER_CYCLE);
  Serial.println(F(" trays)"));
}

void loop() {
  // ArduinoCloud.update() feeds the IWDG and drives the Ethernet/MQTT
  // state machines. With [FIX-20] a dead network costs at most ~3 s per
  // 20 s here instead of 15 s per 15.5 s.
  ArduinoCloud.update();
  unsigned long nowMs = millis();

  // [FIX-23] Periodic offline heartbeat on serial so a technician with a
  // USB cable can see the plant is alive and why the cloud is down.
  {
    static unsigned long nextBeat = 0;
    if (!ArduinoCloud.connected() && timeReached(nextBeat, nowMs)) {
      nextBeat = nowMs + 60000UL;
      IPAddress ip = Ethernet.localIP();
      Serial.print(F("[OFFLINE] up ")); Serial.print(nowMs / 1000UL);
      Serial.print(F(" s, ip ")); Serial.print(ip);
      Serial.print(F(", net state ")); Serial.print((int)StaticEthConnection.getStatus());
      Serial.print(F(", VFD en/flt: "));
      for (uint8_t g = 0; g < NUM_VFD_GROUPS; ++g) {
        Serial.print(vfdGroups[g].driveEnabled ? '1' : '0');
        Serial.print(vfdGroups[g].driveFault   ? 'F' : '-');
        Serial.print(vfdGroups[g].commFault    ? 'X' : ' ');
      }
      Serial.println(F(" (towers keep running)"));
    }
  }

  unsigned long di = dayIndexNow();
  if (di != lastDayIndex) {
    for (uint8_t ch = 0; ch < NUM_CH; ++ch) trigCountArr[ch]=0;
    // [FIX-29] towerIrrCount is NOT reset here: this rollover is uptime
    // based (random time of day) and would restart a run in progress.
    // It is reset when the operator re-arms the tower.
    for (uint8_t i = 0; i < NUM_TOWERS; ++i)
      towerTrayCount[i]=0;
    for (uint8_t g = 0; g < NUM_VFD_GROUPS; ++g)
      vfdGroups[g].ext.peakCurrentPct = 0.0f;

    lastDayIndex = di;
  }

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

  for (uint8_t e = 0; e < NUM_EXPANSIONS; ++e)
    safeUpdateDigitalInputs(e);

  for (uint8_t i = 0; i < NUM_TOWERS; i++)
    readReedAndUpdate(i, nowMs);

  if (!anyIrrigationValveOn()) {
    for (uint8_t i = 0; i < NUM_TOWERS; ++i) {
      Tower &t = towers[i];
      if (!t.fault || t.faultEmailSent || t.bypassLine) continue;
      unsigned long lastAttempt = t.lastFaultEmailAttempt;
      if (lastAttempt==0 || (nowMs-lastAttempt) >= faultEmailRetryMs(t))
        notifyTowerFault(i, (lastAttempt==0), nowMs);
    }
  }

  for (uint8_t i = 0; i < NUM_TOWERS; ++i) {
    if (towers[i].bypassLine) continue;
    bool desiredOn = towerOnEnabled[i] && !towers[i].fault;
    if (towers[i].desiredContactorOn != desiredOn)
      setContactor(i, desiredOn);
  }

  processIntermittentMode(nowMs);

  for (uint8_t g = 0; g < NUM_VFD_GROUPS; ++g)
    processVfdGroupStateMachine(g, nowMs);

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

      if (channelIrrigationEnabled(ch))
        fireValve(exp1, ch, nowMs);
      else {
        firedThisAssert[ch]=true; ignoreUntilMs[ch]=nowMs+IGNORE_MS;
      }
    }
  }

  for (uint8_t ch = 0; ch < NUM_CH; ++ch) {
    if (lastStable[ch]==ACTIVE_LEVEL && inputActiveStartMs[ch]!=0 &&
        !stuckSensorFault[ch] && (nowMs-inputActiveStartMs[ch]) >= STUCK_SENSOR_MS) {
      stuckSensorFault[ch]=true;
      Serial.print(F("[STUCK] Irr ch ")); Serial.print(ch);
      Serial.println(F(" flagged."));
    }
  }

  for (uint8_t ch = 0; ch < NUM_CH; ++ch) {
    if (!channelIrrigationEnabled(ch)) continue;
    if (lastStable[ch]!=ACTIVE_LEVEL) continue;
    if (outStateArr[ch]) continue;
    if (firedThisAssert[ch]) continue;
    if (stuckSensorFault[ch]) continue;
    if (ignoreUntilMs[ch] && !timeReached(ignoreUntilMs[ch], nowMs)) continue;
    if (lastTrigMsArr[ch]==0) continue;
    fireValve(exp1, ch, nowMs);
  }

  pollVfdsIfDue(nowMs);

  publishIrrigationToCloud(nowMs);
  publishTowersToCloud(nowMs);
  publishVfdTelemetryToCloud(nowMs);
#if ENABLE_GRODAN
  bool grodanBusy = anyIrrigationValveOn()
                 || vfdGroups[0].state != VGS_RUNNING
                 || vfdGroups[1].state != VGS_RUNNING;
  GrodanSensors::update(nowMs, grodanBusy);
#endif
}
