import time
import json
import socket
import paho.mqtt.client as mqtt
import RPi.GPIO as GPIO
import adafruit_dht
import board
import busio
import digitalio
import adafruit_max31865
import adafruit_ads1x15.ads1115 as ADS
from adafruit_ads1x15.analog_in import AnalogIn
import minimalmodbus
import smbus2
import os
import sys # For sys.exit()

# === Device Info ===
HOSTNAME = socket.gethostname()
LOG_FILE_PATH = f"/var/log/{HOSTNAME}_algae_sensor.log" # Centralized log file

# === Logging Setup ===
def log_message(level, message):
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    log_entry = f"[{timestamp}] [{HOSTNAME}] [{level}] {message}"
    print(log_entry) # Also print to console for immediate feedback

    try:
        with open(LOG_FILE_PATH, "a") as f:
            f.write(log_entry + "\n")
    except Exception as e:
        print(f"[{timestamp}] [ERROR] Failed to write to log file {LOG_FILE_PATH}: {e}")

# === Load Config ===
CONFIG_PATH = f"/home/{HOSTNAME}/config/{HOSTNAME}.json"
config = {}
try:
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)
        log_message("INFO", "Config loaded.")
except FileNotFoundError:
    log_message("ERROR", f"Config file not found at {CONFIG_PATH}. Exiting.")
    sys.exit(1)
except json.JSONDecodeError as e:
    log_message("ERROR", f"Config file JSON decode error: {e}. Exiting.")
    sys.exit(1)
except Exception as e:
    log_message("ERROR", f"Config load error: {e}. Exiting.")
    sys.exit(1)

# === MQTT Setup ===
mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
try:
    mqttc.username_pw_set(config["mqtt_user"], config["mqtt_pass"])
    mqttc.connect(config["mqtt_host"], config["mqtt_port"])
except KeyError as e:
    log_message("ERROR", f"Missing MQTT credential in config: {e}. Exiting.")
    sys.exit(1)
except Exception as e:
    log_message("ERROR", f"MQTT connection failed: {e}. Exiting.")
    sys.exit(1)


def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        log_message("INFO", "MQTT connected successfully.")
        # Subscribe with QoS 1 for commands to ensure delivery
        client.subscribe(f"algae_farm/{HOSTNAME}/relay/+", qos=1)
    else:
        log_message("ERROR", f"MQTT connection failed with code {rc}.")

def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode().strip().lower()
        log_message("INFO", f"Received MQTT message on topic '{topic}': '{payload}'")

        if topic.startswith(f"algae_farm/{HOSTNAME}/relay/"):
            # Extract relay number from topic: e.g., algae_farm/pi0-01/relay/1 -> 1
            parts = topic.split("/")
            if len(parts) >= 4 and parts[3].isdigit():
                relay_num = int(parts[3])
            else:
                log_message("WARNING", f"Could not parse relay number from topic: {topic}")
                return

            pin = RELAY_PINS.get(f"relay{relay_num}")
            if pin is None:
                log_message("WARNING", f"Relay number {relay_num} not mapped to a GPIO pin.")
                return

            # Control logic for specific relays
            if relay_num == 1:  # Valves (assuming HIGH activates, LOW deactivates)
                GPIO.output(pin, GPIO.HIGH if payload == "on" else GPIO.LOW)
            elif relay_num == 2:  # Pump (assuming HIGH activates, LOW deactivates)
                GPIO.output(pin, GPIO.HIGH if payload == "on" else GPIO.LOW)
            elif relay_num == 7:  # CO2 (momentary press only)
                if payload == "on":
                    GPIO.output(pin, GPIO.LOW) # Assuming LOW triggers the press
                    time.sleep(1) # Duration of the press
                    GPIO.output(pin, GPIO.HIGH) # Release the press
                    log_message("INFO", f"CO2 relay {relay_num} momentarily pressed.")
                else:
                    log_message("WARNING", f"CO2 relay {relay_num} received '{payload}' payload, expecting 'on' for momentary press.")
            else: # General relays (assuming LOW activates, HIGH deactivates)
                GPIO.output(pin, GPIO.LOW if payload == "on" else GPIO.HIGH)
            
            log_message("INFO", f"Relay {relay_num} (GPIO{pin}) set to '{payload}'.")
        else:
            log_message("WARNING", f"Unhandled MQTT topic: {topic}")

    except Exception as e:
        log_message("ERROR", f"Relay control error: {e}")

mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.loop_start()

# === Relay Setup ===
RELAY_PINS = {
    "relay1": 5,
    "relay2": 6,
    "relay3": 13,
    "relay4": 19,
    "relay5": 26,
    "relay6": 16,
    "relay7": 20, # CO2 Momentary
    "relay8": 21  # Fan (Auto-controlled)
}
GPIO.setmode(GPIO.BCM)
for pin_name, pin_num in RELAY_PINS.items():
    GPIO.setup(pin_num, GPIO.OUT)
    GPIO.output(pin_num, GPIO.HIGH) # Default all relays OFF (HIGH assuming common active LOW relays)
    log_message("INFO", f"GPIO{pin_num} ({pin_name}) initialized to HIGH (OFF).")


relay_defaults = config.get("relays", {})
for i in range(1, 9): # Iterate through all 8 relays
    key = str(i)
    pin = RELAY_PINS.get(f"relay{i}")
    if pin is not None:
        default_state = relay_defaults.get(key, "off").lower()
        if i == 1 or i == 2: # Valves & Pump (HIGH ON / LOW OFF)
            GPIO.output(pin, GPIO.HIGH if default_state == "on" else GPIO.LOW)
        elif i == 7: # CO2 momentary, no persistent "on" state
            GPIO.output(pin, GPIO.HIGH) # Ensure it's off by default
        else: # Other relays (LOW ON / HIGH OFF)
            GPIO.output(pin, GPIO.LOW if default_state == "on" else GPIO.HIGH)
        log_message("INFO", f"Relay {i} (GPIO{pin}) set to initial state '{default_state}'.")
    else:
        log_message("WARNING", f"Relay {i} not found in RELAY_PINS definition.")


# === Sensor Setup ===
# DHT11 on GPIO17
try:
    dht = adafruit_dht.DHT11(board.D17)
    log_message("INFO", "DHT11 sensor initialized.")
except Exception as e:
    dht = None
    log_message("ERROR", f"DHT11 sensor initialization failed: {e}")

# PT100 with MAX31865 on SPI (3-wire)
try:
    spi = busio.SPI(board.SCK, board.MOSI, board.MISO)
    cs = digitalio.DigitalInOut(board.D8)
    pt100 = adafruit_max31865.MAX31865(spi, cs, rtd_nominal=100, ref_resistor=430.0, wires=3)
    log_message("INFO", "PT100 sensor initialized.")
except Exception as e:
    pt100 = None
    log_message("ERROR", f"PT100 sensor initialization failed: {e}")

# pH sensor on ADS1115 A0
try:
    i2c = busio.I2C(board.SCL, board.SDA) # Primary I2C bus for pH & CO2
    ads = ADS.ADS1115(i2c)
    ph_channel = AnalogIn(ads, ADS.P0)
    log_message("INFO", "pH sensor (ADS1115) initialized.")
except Exception as e:
    ads = None
    ph_channel = None
    log_message("ERROR", f"pH sensor (ADS1115) initialization failed: {e}")

# Light sensors via RS485 (ensure these /dev/ttyACM paths are consistent)
light1 = None
light2 = None
try:
    # Adjust paths if using /dev/serial/by-id/ or by-path/
    light1 = minimalmodbus.Instrument('/dev/ttyACM2', 1)
    light1.serial.baudrate = 9600
    light1.serial.timeout = 1
    light1.mode = minimalmodbus.MODE_RTU
    log_message("INFO", "Light sensor 1 initialized on /dev/ttyACM2.")
except Exception as e:
    log_message("ERROR", f"Light sensor 1 initialization failed on /dev/ttyACM2: {e}")

try:
    # Adjust paths if using /dev/serial/by-id/ or by-path/
    light2 = minimalmodbus.Instrument('/dev/ttyACM3', 1)
    light2.serial.baudrate = 9600
    light2.serial.timeout = 1
    light2.mode = minimalmodbus.MODE_RTU
    log_message("INFO", "Light sensor 2 initialized on /dev/ttyACM3.")
except Exception as e:
    log_message("ERROR", f"Light sensor 2 initialization failed on /dev/ttyACM3: {e}")


# Turbidity sensor via RS485
turbidity = None
try:
    # Adjust paths if using /dev/serial/by-id/ or by-path/
    turbidity = minimalmodbus.Instrument('/dev/ttyACM1', 1)
    turbidity.serial.baudrate = 9600
    turbidity.serial.timeout = 1
    turbidity.mode = minimalmodbus.MODE_RTU
    log_message("INFO", "Turbidity sensor initialized on /dev/ttyACM1.")
except Exception as e:
    log_message("ERROR", f"Turbidity sensor initialization failed on /dev/ttyACM1: {e}")

# CO₂ sensor via I2C (Atlas Scientific EZO)
co2_i2c_bus = None
CO2_SENSOR_ACTIVE = False
try:
    co2_addr = 0x69 # Default address for Atlas EZO CO2
    co2_i2c_bus = smbus2.SMBus(1) # Bus 1 for Raspberry Pi
    CO2_SENSOR_ACTIVE = True
    log_message("INFO", "CO2 sensor (Atlas Scientific EZO) I2C bus initialized.")
except Exception as e:
    log_message("ERROR", f"CO2 sensor (Atlas Scientific EZO) I2C bus initialization failed: {e}")


# === Read Functions ===
def read_dht():
    if dht is None:
        return None, None # Return None for no sensor
    try:
        temp_c = dht.temperature
        hum = dht.humidity
        if temp_c is not None:
            temp_f = round(temp_c * 9 / 5 + 32, 1) # Convert to Fahrenheit
            return temp_f, round(hum, 1)
        return None, None
    except Exception as e:
        log_message("ERROR", f"DHT11 read error: {e}")
        return None, None

def read_pt100():
    if pt100 is None:
        return None
    try:
        return round(pt100.temperature, 2)
    except Exception as e:
        log_message("ERROR", f"PT100 read error: {e}")
        return None

def read_ph():
    if ph_channel is None:
        return None
    try:
        # Assuming 3.5 * voltage is your calibration, adjust if necessary
        return round(3.5 * ph_channel.voltage, 2)
    except Exception as e:
        log_message("ERROR", f"pH read error: {e}")
        return None

def read_turbidity():
    if turbidity is None:
        return None
    try:
        return turbidity.read_register(1, 2) # Check register and function code if needed
    except Exception as e:
        log_message("ERROR", f"Turbidity read error: {e}")
        return None

def read_light(sensor):
    if sensor is None:
        return None
    try:
        # These register values might need adjustment based on specific sensor model
        high = sensor.read_register(2, 0, 3) # Register 2, function code 3 (read holding register)
        low = sensor.read_register(3, 0, 3) # Register 3
        # Assuming lux calculation: (high << 16) + low / 1000
        return round(((high << 16) + low) / 1000, 2)
    except Exception as e:
        log_message("ERROR", f"Light sensor read error: {e}")
        return None

def read_co2_i2c():
    if not CO2_SENSOR_ACTIVE or co2_i2c_bus is None:
        return None
    try:
        # Send the 'R' (Read) command
        # Atlas Scientific EZO modules typically use 0x01 for the command register
        co2_i2c_bus.write_i2c_block_data(co2_addr, 0x01, [ord('R')])
        time.sleep(1.0) # Atlas CO2 EZO needs approx 0.9 seconds for reading

        # Read the response (up to 32 bytes)
        # The first byte is the status byte, subsequent bytes are data.
        # Reading from the data register (0x01)
        data = co2_i2c_bus.read_i2c_block_data(co2_addr, 0x01, 32)

        status_byte = data[0]
        if status_byte == 0x01: # Success
            # Decode the rest of the bytes into a string
            # Strips null terminators and non-printable characters
            response_str = bytearray(data[1:]).decode('ascii').strip('\x00\r\n\t ')
            try:
                co2_value = float(response_str)
                return round(co2_value, 2)
            except ValueError:
                log_message("ERROR", f"CO2: Could not convert '{response_str}' to float. Raw data: {data[1:]}")
                return None # Return None for parsing error
        elif status_byte == 0x02:
            log_message("ERROR", f"CO2 Error: Command failed (status 0x02). Check sensor state/calibration.")
            return None
        elif status_byte == 0xFE:
            log_message("WARNING", f"CO2 Error: Still processing (status 0xFE). Consider increasing sleep time or retry.")
            return None # Or you might want to retry
        elif status_byte == 0xFF:
            log_message("WARNING", f"CO2 Error: No data to send (status 0xFF).")
            return None
        else:
            log_message("ERROR", f"CO2 Error: Unknown status byte 0x{status_byte:02x}. Raw data: {data}")
            return None

    except FileNotFoundError:
        log_message("ERROR", f"CO2 I2C Error: /dev/i2c-1 not found. Ensure I2C is enabled and connected.")
        return None
    except Exception as e:
        log_message("ERROR", f"CO2 Reading Error: {e}")
        return None

# === Main Loop ===
log_message("INFO", "Starting main sensor loop...")
while True:
    try:
        temp, hum = read_dht()
        water_temp = read_pt100()
        ph = read_ph()
        turb = read_turbidity()
        lux1 = read_light(light1)
        lux2 = read_light(light2)
        co2 = read_co2_i2c()

        # Auto-control fan (relay8) - assuming LOW for ON, HIGH for OFF
        if isinstance(water_temp, (int, float)) and water_temp >= 45:
            if GPIO.input(RELAY_PINS["relay8"]) != GPIO.LOW: # Check current state to avoid unnecessary toggles
                GPIO.output(RELAY_PINS["relay8"], GPIO.LOW) # Turn fan ON
                log_message("INFO", f"Water temperature {water_temp}°F, turning relay8 (fan) ON.")
        elif isinstance(water_temp, (int, float)) and water_temp < 45:
            if GPIO.input(RELAY_PINS["relay8"]) != GPIO.HIGH: # Check current state
                GPIO.output(RELAY_PINS["relay8"], GPIO.HIGH) # Turn fan OFF
                log_message("INFO", f"Water temperature {water_temp}°F, turning relay8 (fan) OFF.")
        # If water_temp is None, don't control the fan
        elif water_temp is None:
             log_message("WARNING", "Water temperature is not available, fan auto-control skipped.")


        payload = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "temp_f": temp, # Renamed for clarity
            "humidity": hum,
            "watertemp_c": water_temp, # PT100 gives Celsius, keeping original variable name but note here
            "ph": ph,
            "turbidity": turb,
            "light1_lux": lux1, # Renamed for clarity
            "light2_lux": lux2, # Renamed for clarity
            "co2_ppm": co2 # Renamed for clarity
        }

        log_message("INFO", f"Sensor readings: {payload}")

        # Publish all sensor readings as a single JSON payload
        mqttc.publish(f"algae_farm/sensors/{HOSTNAME}/all", json.dumps(payload), qos=1, retain=False)
        
        # You could also publish individual values if desired for specific subscriptions
        # for key, val in payload.items():
        #     mqttc.publish(f"algae_farm/sensors/{HOSTNAME}/{key}", str(val), qos=0, retain=False)


    except Exception as e:
        log_message("ERROR", f"Main loop error: {e}")

    time.sleep(5)
