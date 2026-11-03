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
async function getActiveSession(payload, passageAt) {
  const { data, error } = await supabase
    .from("rt004_sessions")
    .select("id,test_name,started_at,event_id,device_address")
    .eq("device_address", payload.address)
    .lte("started_at", passageAt)
    .or(`finished_at.is.null,finished_at.gt.${passageAt}`)
    .order("started_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("[SUPABASE] Erro consultando sessão ativa:", error.message);
    return null;
  }
  return data || null;
}

// Resolve a associação temporal válida no instante da passagem.
// O horário é capturado no callback MQTT antes de entrar na fila.
async function getSessionAssignment(sessionId, transponderId, passageAt) {
  const { data, error } = await supabase
    .from("rt004_session_transponders")
    .select("id,transponder_id,rt004_kart_id,portal_kart_id,pilot_id,started_at,ended_at")
    .eq("session_id", sessionId)
    .eq("transponder_id", transponderId)
    .lte("started_at", passageAt)
    .or(`ended_at.is.null,ended_at.gt.${passageAt}`)
    .order("started_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("[SUPABASE] Erro consultando associação temporal:", error.message);
    return null;
  }
  if (!data) return null;

  let kartLabel = null;
  if (data.portal_kart_id) {
    const { data: portalKart, error: kartError } = await supabase
      .from("karts")
      .select("id,number,name")
      .eq("id", data.portal_kart_id)
      .maybeSingle();
    if (kartError) {
      console.error("[SUPABASE] Erro consultando kart do portal:", kartError.message);
      return null;
    }
    if (!portalKart) {
      console.warn(`[LAP] Kart ${data.portal_kart_id} não encontrado; volta será mantida sem kart.`);
    } else {
      kartLabel = `${portalKart.number ?? ""} ${portalKart.name ?? ""}`.trim() || `Kart ${portalKart.id.slice(0, 8)}`;
    }
  }

  return {
    id: data.id,
    transponderId: data.transponder_id,
    rt004KartId: data.rt004_kart_id,
    portalKartId: data.portal_kart_id,
    pilotId: data.pilot_id,
    kartLabel,
  };
}

// Numeração de volta por associação: ao trocar o transponder de kart,
// cada novo período inicia sua própria sequência.
async function getNextLapNumber(sessionId, transponderId) {
  const { data, error } = await supabase
    .from("rt004_laps")
    .select("lap_number")
    .eq("session_id", sessionId)
    .eq("transponder_id", transponderId)
    .order("lap_number", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("[SUPABASE] Erro consultando última volta do transponder:", error.message);
    return null;
  }
  return Number(data?.lap_number || 0) + 1;
}

async function processLap(payload, passageAt) {
  const validationError = validatePayload(payload);
  if (validationError) {
    console.error(`[MQTT] Payload rejeitado: ${validationError}`);
    return;
  }

  const session = await getActiveSession(payload, passageAt);
  if (!session) {
    console.warn(`[LAP] Sem sessão ativa para ${payload.device} (${payload.address}).`);
    return;
  }

  // O transponder precisa existir e estar ativo. Kart e piloto não são requisitos.
  const { data: transponder, error: transponderError } = await supabase
    .from("rt004_transponders")
    .select("transponder_id,active")
    .eq("transponder_id", payload.transponder_id)
    .maybeSingle();

  if (transponderError) {
    console.error("[SUPABASE] Erro consultando transponder:", transponderError.message);
    return;
  }
  if (!transponder?.active) {
    console.warn(`[LAP] Passagem ignorada: transponder ${payload.transponder_id} inexistente ou inativo.`);
    return;
  }

  // A ausência de associação é válida: grava a volta apenas com transponder/sessão.
  const assignment = await getSessionAssignment(
    session.id,
    payload.transponder_id,
    passageAt
  );

  const nextLapNumber = await getNextLapNumber(session.id, payload.transponder_id);
  if (nextLapNumber == null) return;

  const endAt = new Date(passageAt);
  const startAt = new Date(endAt.getTime() - Math.round(payload.lap_time_ms));

  const lapRecord = {
    session_id: session.id,
    session_transponder_id: assignment?.id ?? null,
    lap_number: nextLapNumber,
    transponder_id: payload.transponder_id,
    kart_id: assignment?.rt004KartId || null,
    portal_kart_id: assignment?.portalKartId || null,
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
  console.log(`[LAP] Kart: ${assignment?.kartLabel || "não associado"}`);
  console.log(`[LAP] Piloto: ${assignment?.pilotId || "não associado"}`);
  console.log(`[LAP] Transponder ID: ${payload.transponder_id}`);
  console.log(`[LAP] Associação: ${assignment?.id || "nenhuma"}`);
  console.log(`[LAP] Passagem: ${passageAt}`);
  console.log(`[LAP] Número: ${data.lap_number}; tempo: ${data.lap_time_ms} ms`);
  console.log(`[LAP] Supabase ID: ${data.id}`);
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
console.log("[KART] Kart/piloto opcionais; associação temporal resolvida por sessão/transponder.");
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
  const passageAt = new Date().toISOString(); // instante de recebimento MQTT
  console.log(`[MQTT] Mensagem recebida em ${topic} às ${passageAt}: ${body}`);
  messageQueue = messageQueue
    .then(async () => {
      let payload;
      try {
        payload = JSON.parse(body);
      } catch {
        console.error("[MQTT] Payload rejeitado: JSON inválido.");
        return;
      }
      await processLap(payload, passageAt);
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
      kartMapping: "optional-session-temporal",
      timestamp: nowIso(),
    }));
    return;
  }
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("ADIEM Timing Backend ONLINE\n");
});

server.listen(HTTP_PORT, () => console.log(`[HTTP] Health server na porta ${HTTP_PORT}.`));
