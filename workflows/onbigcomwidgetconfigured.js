export default async function (ctx) {
  const input = ctx.input || {};
  const headers = ctx.headers || {};
  const tenantId = headers["x-fastn-space-tenantid"] ?? "";
  const cfg = input.configuration || input;
  const akeneoEnv = cfg.akeneoEnv ?? null;

  // Static vars (v1 Variable15)
  const syncEntitiesFlow = "cacheAkeneoEntities";
  const syncProductsFlow = "syncProductsToBC";
  const fetchProductsFlow = "fetchAkeneoProductsToDB";
  const verifyMappedValues = true;

  const tableNames = await fastn.envConfig.get("tableNames");
  const util = (tableNames && tableNames.utility) || {};
  const channelMappingsTable = util.channelMappings || null;

  const notes = {};

  // ---------- Channel-mapping verification & reconciliation ----------
  // v1: bcWidgetConfig.output.channels = [{ label: akeneoCode, value: bcChannelId }]
  const configChannels = cfg.channels || (cfg.bigCommerceConfig && cfg.bigCommerceConfig.channels) || [];
  if (verifyMappedValues && channelMappingsTable) {
    const existing = await fastn.db.v1.query(
      `SELECT * FROM ${channelMappingsTable} WHERE tenant_id = $1`, [tenantId]);
    const dbMappings = existing.rows || [];

    const dbMap = {};
    for (const row of dbMappings) dbMap[row.akeneo_channel_code] = row.bc_channel_id;
    const configMap = {};
    for (const ch of configChannels) configMap[ch.label] = parseInt(ch.value, 10);

    const mismatches = [], new_mappings = [], removed_codes = [];
    for (const ch of configChannels) {
      const code = ch.label;
      const bcId = parseInt(ch.value, 10);
      if (dbMap[code] === undefined) new_mappings.push({ akeneo_channel_code: code, bc_channel_id: bcId });
      else if (dbMap[code] !== bcId) mismatches.push({ akeneo_channel_code: code, bc_channel_id_in_config: bcId, bc_channel_id_in_db: dbMap[code] });
    }
    for (const row of dbMappings) {
      if (configMap[row.akeneo_channel_code] === undefined) removed_codes.push(row.akeneo_channel_code);
    }

    // v1 checkMismatches -> InvalidMappingsFound (block on conflicting existing mapping)
    if (mismatches.length > 0) {
      return { userMessage: "Invalid channel mappings found. Some channels are already mapped to a different BigCommerce channel. Please resolve the conflicts before saving.", mismatches };
    }
    // Apply additions
    for (const m of new_mappings) {
      await fastn.db.v1.query(
        `INSERT INTO ${channelMappingsTable} (tenant_id, akeneo_channel_code, bc_channel_id) VALUES ($1, $2, $3)`,
        [tenantId, m.akeneo_channel_code, m.bc_channel_id]);
    }
    // Apply removals
    if (removed_codes.length > 0) {
      await fastn.db.v1.query(
        `DELETE FROM ${channelMappingsTable} WHERE tenant_id = $1 AND akeneo_channel_code = ANY($2::text[])`,
        [tenantId, removed_codes]);
    }
    notes.channelMappings = { added: new_mappings.length, removed: removed_codes.length };
  }

  // ---------- createDatabaseSchema (STUB: no v2 connector) ----------
  // v1 called the custom createDatabaseSchema_v1 connector to provision the tenant's
  // per-env akeneo/bc tables. No v2 equivalent — pending connector-builder.
  notes.pendingCreateDatabaseSchema = { akeneoEnv, reason: "custom v1 'createDatabaseSchema' connector has no v2 equivalent yet." };
  console.warn("onBigComWidgetConfigured: createDatabaseSchema not wired in v2 — pending connector-builder.");

  // ---------- Compute sync schedules (extractScheduleDetails, ported verbatim) ----------
  const webhooks = buildScheduleWebhooks(cfg, { tenantId, akeneoEnv, syncEntitiesFlow, syncProductsFlow, fetchProductsFlow });

  // ---------- Webhook/scheduler creation (STUB: no v2 connector) ----------
  // v1 looped webhooks -> getWebhook / createWebhooksWithRoutes / batchCreateWebhooks
  // (fastn-platform community connectors). No v2 equivalent — surface the computed set.
  notes.pendingWebhookCreation = webhooks;
  console.warn("onBigComWidgetConfigured: webhook/scheduler creation not wired in v2 — pending connector-builder. Computed:", JSON.stringify(webhooks.map((w) => w.webhookId)));

  return { userMessage: "Configurations updated.", _migrationNotes: notes };
}

// ---- ported verbatim from v1 extractScheduleDetails ----
function buildScheduleWebhooks(configs, v) {
  const { tenantId, akeneoEnv, syncEntitiesFlow, syncProductsFlow, fetchProductsFlow } = v;
  const DAY_MAP = { Monday: "1", Tuesday: "2", Wednesday: "3", Thursday: "4", Friday: "5", Saturday: "6", Sunday: "7" };
  function parseOffsetMinutes(tz) {
    const tzStr = tz || "UTC+12:00";
    const sign = tzStr.includes("-") ? -1 : 1;
    const [oh, om] = tzStr.replace("UTC", "").replace("+", "").replace("-", "").split(":").map(Number);
    return sign * (oh * 60 + om);
  }
  function toUTC(timeStr, offsetMinutes) {
    let [time, meridian] = timeStr.split(" ");
    let [h, m] = time.split(":").map(Number);
    if (meridian === "PM" && h !== 12) h += 12;
    if (meridian === "AM" && h === 12) h = 0;
    let total = h * 60 + m - offsetMinutes;
    total = ((total % 1440) + 1440) % 1440;
    return { hours: String(Math.floor(total / 60)), minutes: String(total % 60) };
  }
  function buildSchedule(useBCEnv, freq, timeStr, tzStr, days, flowName, flowPayload) {
    const offsetMinutes = parseOffsetMinutes(tzStr);
    const utc = toUTC(timeStr, offsetMinutes);
    const baseWebhookId = useBCEnv === true ? `${flowName}_${akeneoEnv}_${tenantId}` : `${flowName}_${akeneoEnv}`;
    const result = [];
    function buildEntry(cronFields, index) {
      const webhookId = index === 0 ? baseWebhookId : `${baseWebhookId}_${index + 1}`;
      return { flowName, flowPayload, webhookId, schedule: cronFields, scheduleType: "cron", rate: { time: "100", unit: "minutes" } };
    }
    if (freq === "daily") {
      result.push(buildEntry({ minutes: utc.minutes, hours: utc.hours, dayOfMonth: "*", month: "*", dayOfWeek: "?", year: "*" }, 0));
    } else if (freq === "weekly") {
      days.forEach((day, index) => {
        result.push(buildEntry({ minutes: utc.minutes, hours: utc.hours, dayOfMonth: "?", month: "*", dayOfWeek: DAY_MAP[day], year: "*" }, index));
      });
    }
    return result;
  }
  const entityFreq = (configs.entitySyncFrequency && configs.entitySyncFrequency.value) || "weekly";
  const entityTime = (configs.entitySyncTime && configs.entitySyncTime.value) || "12:00 PM";
  const entityTz = (configs.entitySyncTimezone && configs.entitySyncTimezone.value) || "UTC+12:00";
  const entityDays = (configs.entitySyncDays && configs.entitySyncDays.length > 0) ? configs.entitySyncDays.map((d) => d.value) : ["Wednesday"];
  const productFreq = (configs.productSyncFrequency && configs.productSyncFrequency.value) || "daily";
  const productTime = (configs.productSyncTime && configs.productSyncTime.value) || "12:00 AM";
  const productTz = (configs.productSyncTimezone && configs.productSyncTimezone.value) || "UTC+12:00";
  const productDays = (configs.productSyncDays && configs.productSyncDays.length > 0) ? configs.productSyncDays.map((d) => d.value) : [];
  const entitiesWebhooks = buildSchedule(false, entityFreq, entityTime, entityTz, entityDays, syncEntitiesFlow, { akeneoEnv, exportCategories: true, exportBrands: true });
  const productsWebhooks = buildSchedule(true, productFreq, productTime, productTz, productDays, syncProductsFlow, { akeneoEnv, isResumeMode: false });
  const fetchProductsWebhook = { flowName: fetchProductsFlow, flowPayload: { akeneoEnv, source: "scheduled", fullSync: false, initiateSync: false, scheduledRun: true }, webhookId: `fetchAkProductsMonitor_${akeneoEnv}`, schedule: null, scheduleType: "rate", rate: { time: "1", unit: "hours" } };
  return [...entitiesWebhooks, ...productsWebhooks, fetchProductsWebhook];
}
