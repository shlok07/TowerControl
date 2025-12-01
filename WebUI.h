#pragma once

// Very simple status page – you can customise later
const char INDEX_HTML[] PROGMEM = R"HTML(
<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>GroHERE Tower Controller</title>
  </head>
  <body>
    <h1>GroHERE Tower Controller</h1>
    <p>This is the built-in status page.</p>
    <p>Use the Arduino Cloud dashboard for detailed control and logging.</p>
  </body>
</html>
)HTML";
