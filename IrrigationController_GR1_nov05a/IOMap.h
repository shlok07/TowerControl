#pragma once

#include <Arduino.h>
#include "OptaBlue.h"
using namespace Opta;

// Simple mapping type for expansion I/O
struct Chan {
  uint8_t exp;   // expansion index (0, 1, …)
  uint8_t ch;    // channel index on that expansion (0..7)
};



#include "Config.h"

// We tell the firmware which expansion and which channel
// each tower's reed input and line-contactor relay are on.
//
// Adjust the channel numbers to match your actual wiring.
// Here is a simple example assuming:
//
//  - All reeds on Expansion 0 inputs I0..I7
//  - All contactors on Expansion 0 outputs O0..O7
//
// Chan.exp = expansion index (0 = first expansion)
// Chan.ch  = channel number on that expansion (0–7)

const Chan REED_CH[NUM_TOWERS] = {
  {0, 0},   // Tower 0 reed → Expansion 0, channel 0
  {0, 1},   // Tower 1 reed → Expansion 0, channel 1
  {0, 2},   // Tower 2 reed → Expansion 0, channel 2
  {0, 3},   // Tower 3 reed → Expansion 0, channel 3
  {0, 4},   // Tower 4 reed → Expansion 0, channel 4
  {0, 5},   // Tower 5 reed → Expansion 0, channel 5
  {0, 6},   // Tower 6 reed → Expansion 0, channel 6
  {0, 7}    // Tower 7 reed → Expansion 0, channel 7
};

const Chan OUT_CH[NUM_TOWERS] = {
  {0, 0},   // Tower 0 contactor relay
  {0, 1},   // Tower 1 contactor relay
  {0, 2},   // Tower 2 contactor relay
  {0, 3},   // Tower 3 contactor relay
  {0, 4},   // Tower 4 contactor relay
  {0, 5},   // Tower 5 contactor relay
  {0, 6},   // Tower 6 contactor relay
  {0, 7}    // Tower 7 contactor relay
};
