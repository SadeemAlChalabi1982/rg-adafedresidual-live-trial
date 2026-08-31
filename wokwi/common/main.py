from machine import ADC, I2C, PWM, Pin
import json
import network
import time

from umqtt.simple import MQTTClient


TOPIC_ROOT = "rgaf-sadeem-paper3-live-20260831-v1"
MQTT_HOST = "broker.hivemq.com"  # Public trial broker; contains no private measurements
MQTT_PORT = 1883
WIFI_SSID = "Wokwi-GUEST"
WIFI_PASSWORD = ""
RECONNECT_MIN_MS = 1000
RECONNECT_MAX_MS = 15000
STATUS_INTERVAL_MS = 10000
ADC_PINS = {
    "raw_turbidity": 34,
    "filtered_turbidity": 35,
    "ph": 32,
    "temperature": 33,
    "flow": 36,
    "residual_chlorine": 39,
}

# Visible boot diagnostics on the physical diagram:
# orange = firmware running, blue = Wi-Fi ready, green = MQTT ready.
Pin(27, Pin.OUT).value(1)


class LCD1602:
    def __init__(self, i2c, address=0x27):
        self.i2c, self.address, self.backlight = i2c, address, 0x08
        time.sleep_ms(40)
        for nibble in (3, 3, 3, 2):
            self._write4(nibble, 0)
            time.sleep_ms(4)
        for command in (0x28, 0x0C, 0x06, 0x01):
            self.send(command, 0)

    def _write4(self, nibble, mode):
        value = ((nibble & 15) << 4) | mode | self.backlight
        self.i2c.writeto(self.address, bytes((value, value | 4, value)))

    def send(self, value, mode=0):
        self._write4(value >> 4, mode)
        self._write4(value, mode)

    def line(self, row, text):
        self.send(0x80 | (0, 0x40, 0x14, 0x54)[row])
        for char in (str(text) + " " * 20)[:20]:
            self.send(ord(char), 1)


def station_id():
    b0 = Pin(18, Pin.IN, Pin.PULL_UP).value() == 0
    b1 = Pin(19, Pin.IN, Pin.PULL_UP).value() == 0
    return ("austin", "tongji", "virtual")[1 if b0 else 2 if b1 else 0]


def topic(kind):
    return (TOPIC_ROOT + "/" + kind + "/" + station).encode()


def adc_value(name):
    ratio = adcs[name].read_u16() / 65535.0
    if name in ("raw_turbidity", "filtered_turbidity"):
        return 0.03 + 99.97 * ratio
    if name == "ph":
        return 4.0 + 6.0 * ratio
    if name == "temperature":
        return 5.0 + 35.0 * ratio
    if name == "flow":
        return 300.0 + 1700.0 * ratio
    return 0.02 + 0.78 * ratio


def servo(pwm, percent):
    percent = min(100.0, max(0.0, float(percent)))
    pwm.duty_ns(int(500 + 19 * percent) * 1000)


def set_pumps(alum, chlorine):
    global alum_percent, chlorine_percent
    alum_percent = min(100.0, max(0.0, float(alum)))
    chlorine_percent = min(100.0, max(0.0, float(chlorine)))
    servo(alum_pwm, alum_percent)
    servo(chlorine_pwm, chlorine_percent)
    alum_led.value(alum_percent > 1)
    chlorine_led.value(chlorine_percent > 1)
    alarm_led.value(max(alum_percent, chlorine_percent) >= 90)


def display(mode):
    lcd.line(0, "%s R%d" % (station, global_round))
    lcd.line(1, "Raw %.2f F %.2f" % (sensors["raw_turbidity"], sensors["filtered_turbidity"]))
    lcd.line(2, "pH %.2f Cl %.2f" % (sensors["ph"], sensors["residual_chlorine"]))
    lcd.line(3, "%s A%.0f C%.0f" % (mode, alum_percent, chlorine_percent))


def publish_telemetry(source):
    payload = {
        "station": station,
        "sequence": sequence,
        "source": source,
        "microcontroller": "ESP32-WROOM-32E-N8",
        "sensors": sensors,
        "pumps": {"alum": alum_percent, "chlorine": chlorine_percent},
        "global_round": global_round,
        "uptime_ms": time.ticks_ms(),
    }
    client.publish(topic("telemetry"), json.dumps(payload).encode(), qos=0)


def on_message(received, body):
    global sequence, global_round, injected
    try:
        doc = json.loads(body.decode())
        if received == topic("inject"):
            sequence = int(doc.get("sequence", sequence + 1))
            for key in sensors:
                if key in doc:
                    sensors[key] = float(doc[key])
            injected = True
            publish_telemetry(doc.get("origin", "python_edge_simulator"))
            display("SENSE")
        elif received == topic("command"):
            global_round = int(doc.get("global_round", global_round))
            set_pumps(doc.get("alum_percent", 0), doc.get("chlorine_percent", 0))
            display("REGULATE")
            print("PUMPS A=%.1f%% C=%.1f%%" % (alum_percent, chlorine_percent))
        elif received == topic("weights"):
            global_round = int(doc.get("global_round", global_round))
            display("WEIGHTS RX")
            print("GLOBAL MODEL", doc.get("weights_hash", "—"), "ROUND", global_round)
    except Exception as error:
        print("MQTT payload error", error)


def connect_wifi():
    if wlan.isconnected():
        return
    online_led.value(0)
    display("WIFI RETRY")
    print("WIFI connecting to", WIFI_SSID)
    try:
        wlan.disconnect()
    except Exception:
        pass
    wlan.active(True)
    wlan.connect(WIFI_SSID, WIFI_PASSWORD)
    started = time.ticks_ms()
    while not wlan.isconnected():
        if time.ticks_diff(time.ticks_ms(), started) >= 12000:
            raise OSError("WiFi connection timeout")
        time.sleep_ms(150)
    print("WIFI connected", wlan.ifconfig()[0])


def connect_mqtt():
    global client
    session_suffix = time.ticks_ms() & 0xFFFF
    client_id = ("rgaf-%s-%04x" % (station, session_suffix)).encode()
    client = MQTTClient(client_id, MQTT_HOST, port=MQTT_PORT, keepalive=30)
    client.set_callback(on_message)
    client.set_last_will(topic("status"), b"offline", retain=True, qos=0)
    client.connect(clean_session=True)
    for name in ("inject", "command", "weights"):
        client.subscribe(topic(name), qos=0)
    client.publish(topic("status"), b"online", retain=True, qos=0)
    online_led.value(1)
    display("MQTT ONLINE")
    print("MQTT connected", MQTT_HOST, station)
    print("ONLINE", station, "ESP32 -> Raspberry Pi -> Federated Cloud")


def drop_mqtt():
    global client
    online_led.value(0)
    if client is not None:
        try:
            client.disconnect()
        except Exception:
            pass
    client = None


station = station_id()
print("BOOT RG-AdaFedResidual station", station)
adcs = {name: ADC(Pin(pin)) for name, pin in ADC_PINS.items()}
for adc in adcs.values():
    try:
        adc.atten(ADC.ATTN_11DB)
    except AttributeError:
        pass
sensors = {name: 0.0 for name in ADC_PINS}
alum_pwm, chlorine_pwm = PWM(Pin(25), freq=50), PWM(Pin(26), freq=50)
online_led, alum_led = Pin(2, Pin.OUT), Pin(27, Pin.OUT)
chlorine_led, alarm_led = Pin(14, Pin.OUT), Pin(13, Pin.OUT)
lcd = LCD1602(I2C(0, sda=Pin(21), scl=Pin(22), freq=400000))
sequence = global_round = 0
alum_percent = chlorine_percent = 0.0
injected = False

wlan = network.WLAN(network.STA_IF)
client = None
set_pumps(0, 0)

last_local = time.ticks_ms()
last_status = time.ticks_ms()
retry_ms = RECONNECT_MIN_MS
while True:
    if client is None:
        try:
            connect_wifi()
            connect_mqtt()
            retry_ms = RECONNECT_MIN_MS
            last_status = time.ticks_ms()
        except Exception as error:
            drop_mqtt()
            display("RECONNECT")
            print("RECONNECT in %dms:" % retry_ms, error)
            time.sleep_ms(retry_ms)
            retry_ms = min(RECONNECT_MAX_MS, retry_ms * 2)
            continue
    try:
        if not wlan.isconnected():
            raise OSError("WiFi link lost")
        client.check_msg()
        if not injected and time.ticks_diff(time.ticks_ms(), last_local) >= 1500:
            last_local = time.ticks_ms()
            sequence += 1
            for name in ADC_PINS:
                sensors[name] = adc_value(name)
            sensors["raw_delta"] = 0.0
            publish_telemetry("wokwi_electrical_sensor_emulator")
            display("LOCAL ADC")
        if time.ticks_diff(time.ticks_ms(), last_status) >= STATUS_INTERVAL_MS:
            client.publish(topic("status"), b"online", retain=True, qos=0)
            last_status = time.ticks_ms()
        time.sleep_ms(20)
    except Exception as error:
        print("LINK LOST", error)
        drop_mqtt()
        display("RECONNECT")
