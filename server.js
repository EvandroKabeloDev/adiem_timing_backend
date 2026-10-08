const mqtt = require("mqtt");
const { createClient } = require("@supabase/supabase-js");
const http = require("http");
const crypto = require("crypto");

const MQTT_HOST = process.env.MQTT_HOST || "91.108.125.144";
const MQTT_PORT = Number(process.env.MQTT_PORT || 1883);
const MQTT_TOPIC = process.env.MQTT_TOPIC || "adiem/timing/rt004/lap";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const HTTP_PORT = Number(process.env.PORT || 3000);

if (!SUPABASE_URL) {
  console.error("[CONFIG] SUPABASE_URL não configurada.");
  process.exit(1);
}

if (!SUPABASE_SERVICE_ROLE_KEY) {
  console.error("[CONFIG] SUPABASE_SERVICE_ROLE_KEY não configurada.");
  process.exit(1);
}

const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
);

const sessions = new Map();

function nowIso() {
  return new Date().toISOString();
}

function generateSessionId() {
  return crypto.randomUUID();
}

function getSessionKey(payload) {
  return payload.address || payload.device || "unknown-device";
}

async function createSession(payload) {
  const sessionId = generateSessionId();

  const deviceName = payload.device || "RT004";
  const deviceAddress = payload.address || null;

  const { error } = await supabase
    .from("rt004_sessions")
    .insert({
      id: sessionId,
      started_at: nowIso(),
      device_name: deviceName,
      device_address: deviceAddress,
      test_name: "ESP32 MQTT",
      notes: "Sessão criada automaticamente pelo backend MQTT."
    });

  if (error) {
    console.error("[SUPABASE] Erro criando sessão:");
    console.error(error);

    return null;
  }

  sessions.set(getSessionKey(payload), sessionId);

  console.log("========================================");
  console.log("[SESSION] Nova sessão criada.");
  console.log(`[SESSION] ID: ${sessionId}`);
  console.log(`[SESSION] Device: ${deviceName}`);
  console.log(`[SESSION] Address: ${deviceAddress}`);
  console.log("========================================");

  return sessionId;
}

async function getOrCreateSession(payload) {
  const key = getSessionKey(payload);

  if (sessions.has(key)) {
    return sessions.get(key);
  }

  return await createSession(payload);
}

function validatePayload(payload) {
  if (!payload || typeof payload !== "object") {
    return "Payload não é um objeto JSON.";
  }

  if (!payload.device) {
    return "Campo 'device' ausente.";
  }

  if (!payload.address) {
    return "Campo 'address' ausente.";
  }

  if (
    typeof payload.lap_number !== "number" ||
    payload.lap_number < 1
  ) {
    return "Campo 'lap_number' inválido.";
  }

  if (
    typeof payload.lap_time_ms !== "number" ||
    payload.lap_time_ms <= 0
  ) {
    return "Campo 'lap_time_ms' inválido.";
  }

  return null;
}

async function processLap(payload) {
  const validationError = validatePayload(payload);

  if (validationError) {
    console.error(`[MQTT] Payload rejeitado: ${validationError}`);
    return;
  }

  const sessionId = await getOrCreateSession(payload);

  if (!sessionId) {
    console.error("[MQTT] Não foi possível obter sessão.");
    return;
  }

  const endAt = new Date();
  const startAt = new Date(
    endAt.getTime() - payload.lap_time_ms
  );

  const lapRecord = {
    session_id: sessionId,
    lap_number: payload.lap_number,
    start_at: startAt.toISOString(),
    end_at: endAt.toISOString(),
    lap_time_ms: Math.round(payload.lap_time_ms),
    counter_start:
      typeof payload.counter_start === "number"
        ? payload.counter_start
        : null,
    counter_end:
      typeof payload.counter_end === "number"
        ? payload.counter_end
        : null,
    counter_delta:
      typeof payload.counter_delta === "number"
        ? payload.counter_delta
        : null,
    source: payload.source || "esp32_mqtt"
  };

  const { data, error } = await supabase
    .from("rt004_laps")
    .insert(lapRecord)
    .select()
    .single();

  if (error) {
    console.error("[SUPABASE] Erro inserindo volta:");
    console.error(error);
    return;
  }

  console.log("========================================");
  console.log("[LAP] VOLTA RECEBIDA");
  console.log(`[LAP] Session: ${sessionId}`);
  console.log(`[LAP] Número: ${data.lap_number}`);
  console.log(`[LAP] Tempo: ${data.lap_time_ms} ms`);
  console.log(`[LAP] Counter delta: ${data.counter_delta}`);
  console.log(`[LAP] Supabase ID: ${data.id}`);
  console.log("[LAP] SUPABASE OK");
  console.log("========================================");
}

const mqttUrl = `mqtt://${MQTT_HOST}:${MQTT_PORT}`;

console.log("========================================");
console.log("ADIEM TIMING BACKEND");
console.log("========================================");
console.log(`[MQTT] Broker: ${mqttUrl}`);
console.log(`[MQTT] Topic: ${MQTT_TOPIC}`);
console.log(`[SUPABASE] URL: ${SUPABASE_URL}`);
console.log("========================================");

const mqttClient = mqtt.connect(mqttUrl, {
  clientId: `ADIEM-BACKEND-${crypto.randomBytes(4).toString("hex")}`,
  clean: true,
  reconnectPeriod: 5000,
  connectTimeout: 10000,
  keepalive: 30
});

mqttClient.on("connect", () => {
  console.log("[MQTT] CONECTADO AO BROKER.");

  mqttClient.subscribe(
    MQTT_TOPIC,
    { qos: 0 },
    (error) => {
      if (error) {
        console.error("[MQTT] Erro ao assinar tópico:");
        console.error(error);
        return;
      }

      console.log(`[MQTT] SUBSCRITO: ${MQTT_TOPIC}`);
      console.log("[SYSTEM] Backend pronto.");
    }
  );
});

mqttClient.on("reconnect", () => {
  console.log("[MQTT] Tentando reconectar...");
});

mqttClient.on("close", () => {
  console.log("[MQTT] Conexão fechada.");
});

mqttClient.on("error", (error) => {
  console.error("[MQTT] Erro:");
  console.error(error.message);
});

mqttClient.on("message", async (topic, message) => {
  console.log("========================================");
  console.log("[MQTT] MENSAGEM RECEBIDA");
  console.log(`[MQTT] Topic: ${topic}`);
  console.log(`[MQTT] Payload: ${message.toString()}`);

  try {
    const payload = JSON.parse(message.toString());

    await processLap(payload);
  } catch (error) {
    console.error("[MQTT] Erro processando mensagem:");
    console.error(error);
  }
});

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, {
      "Content-Type": "application/json"
    });

    res.end(
      JSON.stringify({
        status: "ok",
        service: "adiem-timing-backend",
        mqtt: mqttClient.connected,
        timestamp: nowIso()
      })
    );

    return;
  }

  res.writeHead(200, {
    "Content-Type": "text/plain; charset=utf-8"
  });

  res.end("ADIEM Timing Backend ONLINE\n");
});

server.listen(HTTP_PORT, () => {
  console.log(`[HTTP] Health server na porta ${HTTP_PORT}.`);
});
