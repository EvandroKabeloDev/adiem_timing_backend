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

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

function nowIso() {
  return new Date().toISOString();
}

function validatePayload(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return "Payload não é um objeto JSON.";
  }
  if (!payload.device) return "Campo 'device' ausente.";
  if (!payload.address) return "Campo 'address' ausente.";
  if (typeof payload.lap_time_ms !== "number" || !Number.isFinite(payload.lap_time_ms) || payload.lap_time_ms <= 0) {
    return "Campo 'lap_time_ms' inválido.";
  }
  return null;
}

// A sessão ativa é sempre consultada no banco. Assim, reiniciar o backend
// não mantém um ID antigo em memória nem envia voltas para uma sessão encerrada.
async function getActiveSession(payload) {
  const { data, error } = await supabase
    .from("rt004_sessions")
    .select("id,test_name,started_at,event_id,device_address")
    .eq("device_address", payload.address)
    .is("finished_at", null)
    .order("started_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("[SUPABASE] Erro consultando sessão ativa:", error.message);
    return null;
  }
  return data || null;
}

async function getNextLapNumber(sessionId) {
  const { data, error } = await supabase
    .from("rt004_laps")
    .select("lap_number")
    .eq("session_id", sessionId)
    .order("lap_number", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("[SUPABASE] Erro consultando última volta:", error.message);
    return null;
  }
  return Number(data?.lap_number || 0) + 1;
}

async function processLap(payload) {
  const validationError = validatePayload(payload);
  if (validationError) {
    console.error(`[MQTT] Payload rejeitado: ${validationError}`);
    return;
  }

  const session = await getActiveSession(payload);
  if (!session) {
    console.warn(`[LAP] Passagem ignorada: nenhuma sessão ativa para ${payload.device} (${payload.address}). Inicie o treino no Live Timing.`);
    return;
  }

  const nextLapNumber = await getNextLapNumber(session.id);
  if (nextLapNumber == null) return;

  const endAt = new Date();
  const startAt = new Date(endAt.getTime() - Math.round(payload.lap_time_ms));
  const lapRecord = {
    session_id: session.id,
    lap_number: nextLapNumber,
    start_at: startAt.toISOString(),
    end_at: endAt.toISOString(),
    lap_time_ms: Math.round(payload.lap_time_ms),
    counter_start: typeof payload.counter_start === "number" ? payload.counter_start : null,
    counter_end: typeof payload.counter_end === "number" ? payload.counter_end : null,
    counter_delta: typeof payload.counter_delta === "number" ? payload.counter_delta : null,
    source: payload.source || "esp32_mqtt",
  };

  const { data, error } = await supabase
    .from("rt004_laps")
    .insert(lapRecord)
    .select()
    .single();

  if (error) {
    console.error("[SUPABASE] Erro inserindo volta:", error.message);
    return;
  }

  console.log("========================================");
  console.log("[LAP] VOLTA RECEBIDA E GRAVADA");
  console.log(`[LAP] Sessão: ${session.id} (${session.test_name || "sem nome"})`);
  console.log(`[LAP] Número do dispositivo: ${payload.lap_number ?? "n/d"}; número da sessão: ${data.lap_number}`);
  console.log(`[LAP] Tempo: ${data.lap_time_ms} ms`);
  console.log(`[LAP] Supabase ID: ${data.id}`);
  console.log("[LAP] SUPABASE OK");
  console.log("========================================");
}

const mqttUrl = `mqtt://${MQTT_HOST}:${MQTT_PORT}`;
console.log("========================================");
console.log("ADIEM TIMING BACKEND — SESSION-AWARE");
console.log("========================================");
console.log(`[MQTT] Broker: ${mqttUrl}`);
console.log(`[MQTT] Topic: ${MQTT_TOPIC}`);
console.log(`[SUPABASE] URL: ${SUPABASE_URL}`);
console.log("[SESSION] As sessões passam a ser iniciadas/encerradas pelo Live Timing.");
console.log("========================================");

const mqttClient = mqtt.connect(mqttUrl, {
  clientId: `ADIEM-BACKEND-${crypto.randomBytes(4).toString("hex")}`,
  clean: true,
  reconnectPeriod: 5000,
  connectTimeout: 10000,
  keepalive: 30,
});

mqttClient.on("connect", () => {
  console.log("[MQTT] CONECTADO AO BROKER.");
  mqttClient.subscribe(MQTT_TOPIC, { qos: 0 }, (error) => {
    if (error) {
      console.error("[MQTT] Erro ao assinar tópico:", error.message);
      return;
    }
    console.log(`[MQTT] SUBSCRITO: ${MQTT_TOPIC}`);
    console.log("[SYSTEM] Backend pronto.");
  });
});

mqttClient.on("reconnect", () => console.log("[MQTT] Tentando reconectar..."));
mqttClient.on("close", () => console.log("[MQTT] Conexão fechada."));
mqttClient.on("error", (error) => console.error("[MQTT] Erro:", error.message));

// Serializa o processamento para duas mensagens próximas não receberem o mesmo número de volta.
let messageQueue = Promise.resolve();
mqttClient.on("message", (topic, message) => {
  const body = message.toString();
  console.log(`[MQTT] Mensagem recebida em ${topic}: ${body}`);
  messageQueue = messageQueue
    .then(async () => processLap(JSON.parse(body)))
    .catch((error) => console.error("[MQTT] Erro processando mensagem:", error.message));
});

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
    res.end(JSON.stringify({ status: "ok", service: "adiem-timing-backend", mqtt: mqttClient.connected, sessionMode: "explicit", timestamp: nowIso() }));
    return;
  }
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("ADIEM Timing Backend ONLINE\n");
});

server.listen(HTTP_PORT, () => console.log(`[HTTP] Health server na porta ${HTTP_PORT}.`));
