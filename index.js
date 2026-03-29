import mqtt from "mqtt";

// ---- ENV ----
const {
  APP_KEY,
  USER_ACCOUNT,
  USER_PASSWORD,

  MQTT_HOST,
  MQTT_PORT = 1883,
  MQTT_USERNAME,
  MQTT_PASSWORD,
  MQTT_TOPIC = "solar/sungrow/power",

  POLL_INTERVAL = 60 * 10, // Every 10 minutes
} = process.env;

const BASE = "https://augateway.isolarcloud.com";
const SYS_CODE = "900";

// ---- core request ----
const post = async (path, body) => {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  const json = await res.json();

  if (json.result_code !== "1") {
    throw new Error(`${path}: ${json.result_msg}`);
  }

  return json.result_data;
};

// ---- api ----
const login = () =>
  post("/v1/userService/login", {
    appkey: APP_KEY,
    sys_code: SYS_CODE,
    user_account: USER_ACCOUNT,
    user_password: USER_PASSWORD,
  }).then(({ token, user_id }) => ({ token, user_id }));

const getPowerStationId = ({ token, user_id }) =>
  post("/v1/powerStationService/getPowerStationList", {
    appkey: APP_KEY,
    sys_code: SYS_CODE,
    token,
    user_id,
    page_no: 1,
    page_size: 10,
  }).then((list) => {
    if (!list?.length) throw new Error("No power stations");
    return { token, ps_id: list[0].ps_id };
  });

const getWatts = ({ token, ps_id }) =>
  post("/v1/powerStationService/getPsDetail", {
    appkey: APP_KEY,
    sys_code: SYS_CODE,
    token,
    ps_id,
  }).then((data) => {
    const power = Number(data.curr_power?.value) || 0.0;
    const unit = data.curr_power?.unit;
    return unit === 'kW' ? power * 1000 : power;
  });

// ---- MQTT (single connection) ----
const mqttClient = mqtt.connect(`mqtt://${MQTT_HOST}`, {
  port: Number(MQTT_PORT),
  username: MQTT_USERNAME,
  password: MQTT_PASSWORD,
});

const publishMqtt = (value) =>
  new Promise((resolve, reject) => {
    mqttClient.publish(
      MQTT_TOPIC,
      String(value),
      { retain: true },
      (err) => (err ? reject(err) : resolve(value))
    );
  });

// ---- state (minimal, pragmatic) ----
let session = null;

const getSession = async () => {
  if (session) return session;
  session = await login().then(getPowerStationId);
  return session;
};

// ---- loop ----
const tick = async () => {
  try {
    const ctx = await getSession();
    const power = await getWatts(ctx);
    await publishMqtt(power);

    console.log(`${new Date().toISOString()} → ${power} W`);
  } catch (err) {
    console.error("Error:", err.message);
    session = null; // force re-login on next tick
  }
};

mqttClient.on("connect", () => {
  console.log("MQTT connected");
  tick();
  setInterval(tick, Number(POLL_INTERVAL) * 1000);
});

mqttClient.on("error", (err) => {
  console.error("MQTT error:", err.message);
});
