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
  if (!Number.isInteger(payload.transponder_id) || payload.transponder_id <= 0) {
    return "Campo 'transponder_id' ausente ou inválido.";
  }
  if (
    typeof payload.lap_time_ms !== "number" ||
    !Number.isFinite(payload.lap_time_ms) ||
    payload.lap_time_ms <= 0
  ) {
    return "Campo 'lap_time_ms' inválido.";
  }
  return null;
}

// Busca a sessão ativa correspondente ao LapWiz que enviou a passagem.
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

// Resolve o ID físico do transponder para o kart cadastrado.
// Não atribui passagens a karts desconhecidos/inativos.
async function getTransponderAssignment(transponderId) {
  const { data, error } = await supabase
    .from("rt004_transponders")
    .select("transponder_id,active,kart_id,rt004_karts!inner(id,code,name,active)")
    .eq("transponder_id", transponderId)
    .eq("active", true)
    .eq("rt004_karts.active", true)
    .maybeSingle();

  if (error) {
    console.error("[SUPABASE] Erro consultando cadastro do transponder:", error.message);
    return null;
  }

  if (!data) return null;

  const kart = data.rt004_karts;
  return {
    transponderId: data.transponder_id,
    kartId: data.kart_id,
    kartCode: kart.code,
    kartName: kart.name,
  };
}

// O backend serializa mensagens MQTT. A numeração é independente por
// sessão + kart, e não global para todos os transponders.
async function getNextLapNumber(sessionId, kartId) {
  const { data, error } = await supabase
    .from("rt004_laps")
    .select("lap_number")
    .eq("session_id", sessionId)
    .eq("kart_id", kartId)
    .order("lap_number", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("[SUPABASE] Erro consultando última volta do kart:", error.message);
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

  const assignment = await getTransponderAssignment(payload.transponder_id);
  if (!assignment) {
    console.warn(
      `[LAP] Passagem ignorada: transponder ID ${payload.transponder_id} não cadastrado ou inativo.`
    );
    return;
  }

  const session = await getActiveSession(payload);
  if (!session) {
    console.warn(
      `[LAP] Passagem ignorada: nenhuma sessão ativa para ${payload.device} (${payload.address}). Inicie o treino no Live Timing.`
    );
    return;
  }

  const nextLapNumber = await getNextLapNumber(session.id, assignment.kartId);
  if (nextLapNumber == null) return;

  const endAt = new Date();
  const startAt = new Date(endAt.getTime() - Math.round(payload.lap_time_ms));

  const lapRecord = {
    session_id: session.id,
    lap_number: nextLapNumber,
    transponder_id: assignment.transponderId,
    kart_id: assignment.kartId,
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
  console.log(`[LAP] Kart: ${assignment.kartCode} — ${assignment.kartName}`);
  console.log(`[LAP] Transponder ID: ${assignment.transponderId}`);
  console.log(`[LAP] Número enviado pelo dispositivo: ${payload.lap_number ?? "n/d"}; número do kart: ${data.lap_number}`);
  console.log(`[LAP] Tempo: ${data.lap_time_ms} ms`);
  console.log(`[LAP] Supabase ID: ${data.id}`);
  console.log("[LAP] SUPABASE OK");
  console.log("========================================");
}

const mqttUrl = `mqtt://${MQTT_HOST}:${MQTT_PORT}`;
console.log("========================================");
console.log("ADIEM TIMING BACKEND — KART-AWARE");
console.log("========================================");
console.log(`[MQTT] Broker: ${mqttUrl}`);
console.log(`[MQTT] Topic: ${MQTT_TOPIC}`);
console.log(`[SUPABASE] URL: ${SUPABASE_URL}`);
console.log("[SESSION] Sessões iniciadas/encerradas pelo Live Timing.");
console.log("[KART] Associação resolvida pelo cadastro rt004_transponders.");
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

// Mantém a ordem de processamento das mensagens recebidas neste processo.
let messageQueue = Promise.resolve();
mqttClient.on("message", (topic, message) => {
  const body = message.toString();
  console.log(`[MQTT] Mensagem recebida em ${topic}: ${body}`);
  messageQueue = messageQueue
    .then(async () => {
      let payload;
      try {
        payload = JSON.parse(body);
      } catch {
        console.error("[MQTT] Payload rejeitado: JSON inválido.");
        return;
      }
      await processLap(payload);
    })
    .catch((error) => console.error("[MQTT] Erro processando mensagem:", error.message));
});

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
    res.end(JSON.stringify({
      status: "ok",
      service: "adiem-timing-backend",
      mqtt: mqttClient.connected,
      sessionMode: "explicit",
      kartMapping: true,
      timestamp: nowIso(),
    }));
    return;
  }
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("ADIEM Timing Backend ONLINE\n");
});

server.listen(HTTP_PORT, () => console.log(`[HTTP] Health server na porta ${HTTP_PORT}.`));
