from machine import ADC, I2C, PWM, Pin
import ds18x20
import json
import network
import onewire
import time

from mqtt_simple import MQTTClient


TOPIC_ROOT = "rgaf-sadeem-paper3-linked-20260909-v1"
MQTT_HOST = "broker.hivemq.com"  # Public trial broker; contains no private measurements
MQTT_PORT = 1883
WIFI_SSID = "Wokwi-GUEST"
WIFI_PASSWORD = ""
RECONNECT_MIN_MS = 100
RECONNECT_MAX_MS = 800
STATUS_INTERVAL_MS = 500
ADC_PINS = {
    "raw_turbidity": 34,
    "filtered_turbidity": 35,
    "ph": 32,
    "flow": 36,
    "residual_chlorine": 39,
}
TEMPERATURE_PIN = 33

class LCD2004:
    """Native blue HD44780 20x4 display through its PCF8574 I2C adapter."""

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
        for char in (str(text).upper() + " " * 20)[:20]:
            self.send(ord(char), 1)

    def render(self, rows):
        for row, text in enumerate(rows):
            self.line(row, text)


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
    if name == "flow":
        return 300.0 + 1700.0 * ratio
    return 0.02 + 0.78 * ratio


def sample_local_sensors():
    """Refresh every physical/emulated sensor channel as one coherent sample."""
    global sequence
    sequence += 1
    for name in ADC_PINS:
        sensors[name] = adc_value(name)
    if temperature_roms:
        # DS18B20 conversion is asynchronous. Read the completed conversion,
        # then start the next one without blocking MQTT for 750 simulated ms.
        try:
            measured_temperature = temperature_bus.read_temp(temperature_roms[0])
            if -20.0 <= measured_temperature <= 85.0:
                sensors["temperature"] = measured_temperature
        except Exception:
            pass
        temperature_bus.convert_temp()
    sensors["raw_delta"] = max(
        0.0, sensors["raw_turbidity"] - sensors["filtered_turbidity"]
    )


def local_control_targets():
    """Compute safe visible setpoints while no fresh cloud command is available."""
    turbidity_load = max(
        0.0, sensors["raw_turbidity"] - sensors["filtered_turbidity"]
    )
    chlorine_deficit = max(0.0, 0.35 - sensors["residual_chlorine"])
    alum = min(88.0, max(8.0, 12.0 + 2.4 * turbidity_load))
    chlorine = min(72.0, max(10.0, 24.0 + 95.0 * chlorine_deficit))
    return alum, chlorine


def servo(pwm, percent):
    """Map a dosing percentage to the servo's full 0-180 degree travel."""
    percent = min(100.0, max(0.0, float(percent)))
    pwm.duty_ns(int(500 + 19 * percent) * 1000)


def status_light(milliseconds=1200):
    """Keep the MQTT/activity lamp visible for the complete current event."""
    global status_hold_until
    online_led.value(1)
    status_hold_until = time.ticks_add(time.ticks_ms(), milliseconds)


def pulse_pump_lights(milliseconds=1000):
    """Illuminate each dosing channel long enough to be clearly observable."""
    global alum_light_until, chlorine_light_until
    now = time.ticks_ms()
    if alum_percent >= 0.5:
        alum_led.value(1)
        alum_light_until = time.ticks_add(now, milliseconds)
    if chlorine_percent >= 0.5:
        chlorine_led.value(1)
        chlorine_light_until = time.ticks_add(now, milliseconds)


def update_pump_lights(now=None):
    if now is None:
        now = time.ticks_ms()
    if time.ticks_diff(now, alum_light_until) >= 0:
        alum_led.value(0)
    if time.ticks_diff(now, chlorine_light_until) >= 0:
        chlorine_led.value(0)
    if time.ticks_diff(now, cloud_uplink_until) >= 0:
        cloud_uplink_signal.value(0)
    if time.ticks_diff(now, cloud_downlink_until) >= 0:
        cloud_downlink_signal.value(0)
    alarm_led.value(max(alum_percent, chlorine_percent) >= 90)


def update_pump_motion(now=None):
    """Continuously reciprocate each metering pump at its commanded amplitude."""
    global pump_cycle_started, last_pump_frame
    if now is None:
        now = time.ticks_ms()
    if time.ticks_diff(now, last_pump_frame) < 70:
        update_pump_lights(now)
        return
    elapsed = time.ticks_diff(now, pump_cycle_started)
    if elapsed < 0 or elapsed >= 2400:
        pump_cycle_started = now
        elapsed = 0
        pulse_pump_lights(1000)
    progress = elapsed / 1200.0
    stroke = progress if progress <= 1.0 else 2.0 - progress
    stroke = min(1.0, max(0.0, stroke))
    servo(alum_pwm, alum_percent * stroke)
    servo(chlorine_pwm, chlorine_percent * stroke)
    last_pump_frame = now
    update_pump_lights(now)


def service_visuals(milliseconds=0):
    """Keep pumps and lamps alive during sensor, cloud, and retry waits."""
    started = time.ticks_ms()
    while True:
        now = time.ticks_ms()
        update_pump_motion(now)
        if time.ticks_diff(now, started) >= milliseconds:
            return
        time.sleep_ms(35)


def set_pumps(alum, chlorine, animate=False, restart=True):
    global alum_percent, chlorine_percent, pump_cycle_started, last_pump_frame
    alum_percent = min(100.0, max(0.0, float(alum)))
    chlorine_percent = min(100.0, max(0.0, float(chlorine)))
    if restart:
        pump_cycle_started = time.ticks_ms()
        last_pump_frame = time.ticks_add(pump_cycle_started, -100)
        servo(alum_pwm, 0)
        servo(chlorine_pwm, 0)
        pulse_pump_lights(1000)
    update_pump_motion()
    if animate:
        display("PUMP RUN")


def display(mode):
    lcd.render(
        (
            "%s R%d" % (station, global_round),
            "RAW %.2f F %.2f" % (sensors["raw_turbidity"], sensors["filtered_turbidity"]),
            "PH %.2f CL %.2f" % (sensors["ph"], sensors["residual_chlorine"]),
            "%s A%.0f C%.0f" % (mode, alum_percent, chlorine_percent),
        )
    )


def publish_telemetry(source):
    global cloud_uplink_until
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
    cloud_uplink_signal.value(1)
    cloud_uplink_until = time.ticks_add(time.ticks_ms(), 900)
    client.publish(topic("telemetry"), json.dumps(payload).encode(), qos=0)
    status_light(900)


def on_message(received, body):
    global sequence, global_round, injected, cloud_command_received
    global cloud_downlink_until
    try:
        doc = json.loads(body.decode())
        if received == topic("inject"):
            sequence = int(doc.get("sequence", sequence + 1))
            for key in sensors:
                if key in doc:
                    sensors[key] = float(doc[key])
            injected = True
            status_light(1200)
            publish_telemetry(doc.get("origin", "python_edge_simulator"))
            display("SENSE")
        elif received == topic("command"):
            global_round = int(doc.get("global_round", global_round))
            requested_alum = float(doc.get("alum_percent", 0))
            requested_chlorine = float(doc.get("chlorine_percent", 0))
            command_mode = str(doc.get("mode", ""))
            cloud_downlink_signal.value(1)
            cloud_downlink_until = time.ticks_add(time.ticks_ms(), 1000)
            # The legacy public MQTT service publishes a zero-output safety
            # interlock whenever fewer than three Wokwi tabs are open. This
            # station also operates as an independent visual plant, so that
            # waiting command must not erase its last valid/local command.
            waiting_interlock = (
                requested_alum <= 0.0
                and requested_chlorine <= 0.0
                and "SAFETY_INTERLOCK_WAITING" in command_mode
            )
            if waiting_interlock:
                cloud_command_received = False
                local_alum, local_chlorine = local_control_targets()
                set_pumps(local_alum, local_chlorine, restart=False)
                display("LOCAL HOLD")
            else:
                cloud_command_received = True
                set_pumps(
                    requested_alum,
                    requested_chlorine,
                    animate=True,
                )
                display("REGULATE")
            status_light(1000)
            print("PUMPS A=%.1f%% C=%.1f%%" % (alum_percent, chlorine_percent))
        elif received == topic("weights"):
            global_round = int(doc.get("global_round", global_round))
            cloud_downlink_signal.value(1)
            cloud_downlink_until = time.ticks_add(time.ticks_ms(), 1000)
            status_light(1500)
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
        service_visuals(20)
    print("WIFI connected", wlan.ifconfig()[0])


def connect_mqtt():
    global client
    display("MQTT CONNECT")
    session_suffix = time.ticks_ms() & 0xFFFF
    client_id = ("rgaf-%s-%04x" % (station, session_suffix)).encode()
    client = MQTTClient(client_id, MQTT_HOST, port=MQTT_PORT, keepalive=30)
    client.set_callback(on_message)
    client.set_last_will(topic("status"), b"offline", retain=True, qos=0)
    # The local client enforces this timeout. It prevents the simulator from
    # remaining forever at WIFI RETRY when a public gateway route is slow.
    client.connect(clean_session=True, timeout=8)
    for name in ("inject", "command", "weights"):
        client.subscribe(topic(name), qos=0)
    client.publish(topic("status"), b"online", retain=True, qos=0)
    # Publish one complete snapshot immediately so the cloud freshness gate
    # does not have to wait for the first injected control-cycle row.
    publish_telemetry("wokwi_startup_snapshot")
    status_light(1800)
    display("MQTT ONLINE")
    print("MQTT connected", MQTT_HOST, station)
    print("ONLINE", station, "ESP32 -> Raspberry Pi -> Federated Cloud")


def drop_mqtt():
    global client, cloud_command_received
    online_led.value(0)
    cloud_uplink_signal.value(0)
    cloud_downlink_signal.value(0)
    cloud_command_received = False
    if client is not None:
        try:
            client.disconnect()
        except Exception:
            pass
    client = None


station = station_id()
print("BOOT RG-AdaFedResidual station", station)
# Begin Wi-Fi negotiation before LCD, sensor and lamp initialization.  Wokwi
# can run several rich circuit tabs at a reduced simulation rate; starting the
# radio first lets that unavoidable visual setup time overlap network startup.
wlan = network.WLAN(network.STA_IF)
wlan.active(True)
wlan.connect(WIFI_SSID, WIFI_PASSWORD)
adcs = {name: ADC(Pin(pin)) for name, pin in ADC_PINS.items()}
for adc in adcs.values():
    try:
        adc.atten(ADC.ATTN_11DB)
    except AttributeError:
        pass
sensors = {name: 0.0 for name in ADC_PINS}
sensors["temperature"] = 25.0
temperature_bus = ds18x20.DS18X20(onewire.OneWire(Pin(TEMPERATURE_PIN)))
temperature_roms = temperature_bus.scan()
alum_pwm, chlorine_pwm = PWM(Pin(25), freq=50), PWM(Pin(26), freq=50)
online_led, alum_led = Pin(2, Pin.OUT), Pin(27, Pin.OUT)
chlorine_led, alarm_led = Pin(14, Pin.OUT), Pin(13, Pin.OUT)
cloud_uplink_signal = Pin(16, Pin.OUT)
cloud_downlink_signal = Pin(17, Pin.OUT)
cloud_uplink_until = 0
cloud_downlink_until = 0
lcd = LCD2004(I2C(0, scl=Pin(22), sda=Pin(21), freq=400000))
sequence = global_round = 0
alum_percent = chlorine_percent = 0.0
injected = False
cloud_command_received = False
status_hold_until = 0
alum_light_until = 0
chlorine_light_until = 0
pump_cycle_started = time.ticks_ms()
last_pump_frame = time.ticks_add(pump_cycle_started, -100)

client = None
set_pumps(0, 0)

# Brief startup lamp test; cloud and pump indications remain non-blocking.
for led in (online_led, alum_led, chlorine_led, alarm_led):
    led.value(1)
service_visuals(100)
for led in (online_led, alum_led, chlorine_led, alarm_led):
    led.value(0)

# Show a genuine sensor-driven PWM stroke immediately at startup. Network
# negotiation can take several seconds on the public gateway, but the local
# acquisition/control path must remain observable during that interval.
sample_local_sensors()
startup_alum, startup_chlorine = local_control_targets()
set_pumps(startup_alum, startup_chlorine, animate=True)
display("LOCAL CTRL")

last_local = time.ticks_ms()
last_status = time.ticks_ms()
last_heartbeat = time.ticks_ms()
heartbeat_state = 1
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
            sample_local_sensors()
            local_alum, local_chlorine = local_control_targets()
            set_pumps(local_alum, local_chlorine, animate=True)
            display("LOCAL CTRL")
            display("RECONNECT")
            print("RECONNECT in %dms:" % retry_ms, error)
            service_visuals(retry_ms)
            retry_ms = min(RECONNECT_MAX_MS, retry_ms * 2)
            continue
    try:
        if not wlan.isconnected():
            raise OSError("WiFi link lost")
        client.check_msg()
        now = time.ticks_ms()
        update_pump_motion(now)
        if not injected and time.ticks_diff(time.ticks_ms(), last_local) >= 1500:
            last_local = time.ticks_ms()
            sample_local_sensors()
            if not cloud_command_received:
                local_alum, local_chlorine = local_control_targets()
                set_pumps(local_alum, local_chlorine, restart=False)
            publish_telemetry("wokwi_electrical_sensor_emulator")
            display("LOCAL ADC")
        if time.ticks_diff(time.ticks_ms(), last_status) >= STATUS_INTERVAL_MS:
            client.publish(topic("status"), b"online", retain=True, qos=0)
            # A status heartbeat alone proves the MQTT session exists, but a
            # synchronized sensor snapshot also keeps the live data gate fresh
            # if an individual QoS-0 injection packet is lost by the public
            # broker route.
            publish_telemetry("wokwi_heartbeat_snapshot")
            status_light(700)
            last_status = time.ticks_ms()
        now = time.ticks_ms()
        update_pump_motion(now)
        if time.ticks_diff(now, last_heartbeat) >= 650:
            if time.ticks_diff(now, status_hold_until) >= 0:
                heartbeat_state = 0 if heartbeat_state else 1
                online_led.value(heartbeat_state)
            else:
                heartbeat_state = 1
                online_led.value(1)
            last_heartbeat = time.ticks_ms()
        time.sleep_ms(20)
    except Exception as error:
        print("LINK LOST", error)
        drop_mqtt()
        display("RECONNECT")
