/*
  esp32_managed_node.ino

  Managed ESP32 firmware for:
      ESP32 Node <-> MQTT over Wi-Fi <-> Raspberry Pi 5

  Required Arduino libraries:
      - PubSubClient
      - ArduinoJson

  Initial installation:
      Flash this sketch by USB.
      Build two firmware binaries for formal experiments:
          v1.0 -> set FW_VERSION to "v1.0"
          v1.1 -> set FW_VERSION to "v1.1"

  MQTT topics:
      publish: esp32/status
      publish: esp32/ack
      subscribe: esp32/command

  Supported commands:
      OTA_UPDATE
      ROLLBACK
      RETRY (handled by Pi by resending OTA)
      ABORT_UPDATE
      RECONNECT
      FAULT_COMM_START
*/

#include <WiFi.h>
#include <PubSubClient.h>
#include <HTTPClient.h>
#include <Update.h>
#include <ArduinoJson.h>

// ---------------------------------------------------------------------------
// EDIT THESE VALUES
// ---------------------------------------------------------------------------

const char* WIFI_SSID = "Chelsea";
const char* WIFI_PASSWORD = "zhangchuhan22";
const char* MQTT_HOST = "172.20.10.2";  // Raspberry Pi IP
const uint16_t MQTT_PORT = 1883;

#define FW_VERSION "v1.1"

// ---------------------------------------------------------------------------

const char* STATUS_TOPIC = "esp32/status";
const char* ACK_TOPIC = "esp32/ack";
const char* COMMAND_TOPIC = "esp32/command";

WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);

String otaStatus = "VALID";
bool checksumOk = true;
bool suppressStatus = false;
bool abortRequested = false;
String lastEvent = "BOOT";

unsigned long lastStatusMs = 0;
const unsigned long STATUS_PERIOD_MS = 2000;

// Store last OTA command for local state reporting/debugging.
String lastOtaUrl = "";
String lastExpectedMd5 = "";
String lastFaultMode = "none";


void publishJson(const char* topic, JsonDocument& doc, bool retained = false) {
  char buffer[768];
  size_t n = serializeJson(doc, buffer, sizeof(buffer));
  mqttClient.publish(topic, reinterpret_cast<const uint8_t*>(buffer), n, retained);
}


void publishAck(const char* action, const char* result, const String& detail = "") {
  JsonDocument doc;
  doc["action"] = action;
  doc["result"] = result;
  doc["detail"] = detail;
  doc["firmware_version"] = FW_VERSION;
  publishJson(ACK_TOPIC, doc, false);
}


void publishStatus(bool force = false) {
  if (suppressStatus && !force) return;

  JsonDocument doc;
  doc["firmware_version"] = FW_VERSION;
  doc["mqtt_connected"] = mqttClient.connected();
  doc["checksum_ok"] = checksumOk;
  doc["ota_status"] = otaStatus;
  doc["last_event"] = lastEvent;
  doc["uptime_ms"] = millis();

  publishJson(STATUS_TOPIC, doc, true);
}


void connectWiFi() {
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  unsigned long start = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - start < 20000) {
    delay(300);
  }
}


void connectMQTT() {
  while (!mqttClient.connected()) {
    String clientId = "esp32-managed-" + String((uint32_t)ESP.getEfuseMac(), HEX);
    if (mqttClient.connect(clientId.c_str())) {
      mqttClient.subscribe(COMMAND_TOPIC, 1);
      lastEvent = "MQTT_CONNECTED";
      publishStatus(true);
    } else {
      delay(1000);
    }
  }
}


/*
  Real OTA transfer with two fault-injection modes:

  fault_mode="corrupt":
      One byte in the downloaded stream is flipped before being written.
      Expected MD5 remains that of the valid image, so Update.end() fails
      integrity verification.

  fault_mode="interrupt":
      The OTA write is deliberately aborted after ~40% of the image has
      actually been transferred/written.

  fault_mode="none":
      Normal OTA.
*/
bool performOta(const String& url, const String& expectedMd5, const String& faultMode) {
  lastOtaUrl = url;
  lastExpectedMd5 = expectedMd5;
  lastFaultMode = faultMode;

  checksumOk = true;
  abortRequested = false;
  otaStatus = "STARTING";
  lastEvent = "OTA_START";
  publishStatus(true);

  HTTPClient http;
  http.setTimeout(15000);

  if (!http.begin(url)) {
    otaStatus = "OTA_FAILED";
    lastEvent = "HTTP_BEGIN_FAILED";
    publishStatus(true);
    return false;
  }

  int code = http.GET();
  if (code != HTTP_CODE_OK) {
    otaStatus = "OTA_FAILED";
    lastEvent = "HTTP_GET_FAILED_" + String(code);
    publishStatus(true);
    http.end();
    return false;
  }

  int totalLength = http.getSize();
  if (totalLength <= 0) {
    otaStatus = "OTA_FAILED";
    lastEvent = "INVALID_CONTENT_LENGTH";
    publishStatus(true);
    http.end();
    return false;
  }

  if (!Update.begin((size_t)totalLength)) {
    otaStatus = "OTA_FAILED";
    lastEvent = "UPDATE_BEGIN_FAILED";
    publishStatus(true);
    http.end();
    return false;
  }

  if (expectedMd5.length() == 32) {
    Update.setMD5(expectedMd5.c_str());
  }

  WiFiClient* stream = http.getStreamPtr();
  uint8_t buf[1024];
  size_t writtenTotal = 0;
  bool corruptedOnce = false;

  otaStatus = "DOWNLOADING";
  publishStatus(true);

  while (http.connected() && writtenTotal < (size_t)totalLength) {
    size_t available = stream->available();
    if (!available) {
      delay(5);
      continue;
    }

    size_t toRead = available;
    if (toRead > sizeof(buf)) toRead = sizeof(buf);

    int n = stream->readBytes(buf, toRead);
    if (n <= 0) continue;

    // S1: mutate one byte after roughly 25% has arrived.
    if (faultMode == "corrupt" && !corruptedOnce &&
        writtenTotal > (size_t)totalLength / 4) {
      buf[0] ^= 0x01;
      corruptedOnce = true;
      lastEvent = "FAULT_CORRUPT_BYTE";
    }

    size_t nWritten = Update.write(buf, (size_t)n);
    writtenTotal += nWritten;

    if (nWritten != (size_t)n) {
      otaStatus = "OTA_FAILED";
      lastEvent = "FLASH_WRITE_FAILED";
      Update.abort();
      http.end();
      publishStatus(true);
      return false;
    }

    // S2: real mid-transfer interruption.
    if (faultMode == "interrupt" &&
        writtenTotal >= (size_t)totalLength * 40 / 100) {
      otaStatus = "INTERRUPTED";
      lastEvent = "FAULT_OTA_INTERRUPTED";
      Update.abort();
      http.end();
      publishStatus(true);
      return false;
    }

    if (abortRequested) {
      otaStatus = "ABORTED";
      lastEvent = "OTA_ABORTED";
      Update.abort();
      http.end();
      publishStatus(true);
      return false;
    }

    mqttClient.loop();
    delay(1);
  }

  bool ok = Update.end(true);
  http.end();

  if (!ok) {
    // With expected MD5 configured, a corrupted stream reaches here as an
    // integrity verification failure.
    checksumOk = false;
    otaStatus = "CHECKSUM_FAILED";
    lastEvent = "OTA_INTEGRITY_FAILURE";
    publishStatus(true);
    return false;
  }

  checksumOk = true;
  otaStatus = "VALID";
  lastEvent = "OTA_SUCCESS_REBOOTING";
  publishStatus(true);
  delay(1200);
  ESP.restart();
  return true;
}


void handleCommand(const String& payload) {
  JsonDocument doc;
  DeserializationError err = deserializeJson(doc, payload);
  if (err) {
    publishAck("UNKNOWN", "ERROR", "Invalid JSON");
    return;
  }

  String action = doc["action"] | "";
  String url = doc["url"] | "";
  String md5 = doc["md5"] | "";
  String faultMode = doc["fault_mode"] | "none";

  if (action == "OTA_UPDATE") {
    publishAck("OTA_UPDATE", "ACCEPTED");
    performOta(url, md5, faultMode);
    return;
  }

  if (action == "ROLLBACK") {
    publishAck("ROLLBACK", "ACCEPTED");
    performOta(url, md5, "none");
    return;
  }

  if (action == "ABORT_UPDATE") {
    abortRequested = true;
    publishAck("ABORT_UPDATE", "ACCEPTED");
    return;
  }

  if (action == "FAULT_COMM_START") {
    // Application-level communication fault: suppress runtime status publishing.
    // Command subscription remains active so the recovery action can be received.
    suppressStatus = true;
    lastEvent = "FAULT_COMMUNICATION_STARTED";
    publishAck("FAULT_COMM_START", "ACCEPTED");
    return;
  }

  if (action == "RECONNECT") {
    suppressStatus = false;

    if (WiFi.status() != WL_CONNECTED) {
      connectWiFi();
    }
    if (!mqttClient.connected()) {
      connectMQTT();
    }

    lastEvent = "COMMUNICATION_RESTORED";
    publishAck("RECONNECT", "ACCEPTED");
    publishStatus(true);
    return;
  }

  if (action == "WAIT" || action == "NO_ACTION") {
    publishAck(action.c_str(), "ACCEPTED");
    return;
  }

  publishAck(action.c_str(), "ERROR", "Unsupported action");
}


void mqttCallback(char* topic, byte* payload, unsigned int length) {
  String msg;
  msg.reserve(length);
  for (unsigned int i = 0; i < length; i++) {
    msg += (char)payload[i];
  }

  if (String(topic) == COMMAND_TOPIC) {
    handleCommand(msg);
  }
}


void setup() {
  Serial.begin(115200);
  delay(500);

  connectWiFi();

  mqttClient.setServer(MQTT_HOST, MQTT_PORT);
  mqttClient.setCallback(mqttCallback);
  mqttClient.setBufferSize(1024);

  connectMQTT();

  lastEvent = "READY";
  otaStatus = "VALID";
  checksumOk = true;
  publishStatus(true);
}


void loop() {
  if (WiFi.status() != WL_CONNECTED) {
    connectWiFi();
  }

  if (!mqttClient.connected()) {
    connectMQTT();
  }

  mqttClient.loop();

  if (millis() - lastStatusMs >= STATUS_PERIOD_MS) {
    lastStatusMs = millis();
    publishStatus(false);
  }

  delay(5);
}
