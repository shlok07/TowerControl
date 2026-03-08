#pragma once

#include <Arduino.h>

// How many towers
static const uint8_t NUM_TOWERS     = 8;

// How many expansion modules (0 = towers, 1 = irrigation)
static const uint8_t NUM_EXPANSIONS = 2;

// Fault tolerance: how late a pulse can be before we call it a fault
static const float LATE_TOLERANCE = 0.30f;  // +30%

// Grace period after start/stop or preset change before fault logic is active
static const unsigned long CHANGE_INHIBIT_MS = 30000UL;  // 30 seconds

// Relay / reed polarity
static const bool RELAY_ACTIVE_HIGH = true;  // change if relays are active-low
static const bool REED_ACTIVE_HIGH  = true;  // change if reeds pull to GND

// Debounce time for reed inputs
static const unsigned long REED_DEBOUNCE_MS = 20UL;   // 20 ms is a good start

// Presets for tower speed (example values)
struct TowerPreset {
  float speed;                // user-facing "speed"
  unsigned long interval_ms;  // expected time between pulses
};

static const TowerPreset PRESETS[3] = {
  { 1.0f, 600000UL },   // 10 min
  { 1.5f, 300000UL },   // 5 min
  { 2.0f, 120000UL }    // 2 min
};

// Ethernet MAC address
static byte ETH_MAC[] = { 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01 };
