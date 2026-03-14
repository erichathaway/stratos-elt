export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  const { run_id } = req.query;
  if (!run_id) { res.status(400).json({ error: 'run_id required' }); return; }

  const SB_URL = process.env.SUPABASE_URL;
  const SB_KEY = process.env.SUPABASE_ANON_KEY;
  if (!SB_URL || !SB_KEY) { res.status(500).json({ error: 'Env vars not set' }); return; }

  const h = {
    'apikey': SB_KEY,
    'Authorization': `Bearer ${SB_KEY}`,
    'Content-Type': 'application/json'
  };

  const enc = encodeURIComponent(run_id);

  try {
    const [blobRes, stageRes, dpacketRes, f1Res, masterRes, optionsRes] = await Promise.all([
      fetch(`${SB_URL}/rest/v1/engine_blobs?run_id=eq.${enc}&blob_type=eq.OUTPUT&order=created_at.desc&limit=1`, {headers: h}),
      fetch(`${SB_URL}/rest/v1/engine_stage_outputs?run_id=eq.${enc}&section=eq.D&order=round.asc,role_key.asc`, {headers: h}),
      fetch(`${SB_URL}/rest/v1/engine_blobs?run_id=eq.${enc}&blob_type=eq.D_packet&order=round.desc&limit=1`, {headers: h}),
      fetch(`${SB_URL}/rest/v1/engine_stage_outputs?run_id=eq.${enc}&section=eq.F1&order=round.asc`, {headers: h}),
      fetch(`${SB_URL}/rest/v1/engine_master_state?run_id=eq.${enc}&limit=1`, {headers: h}),
      fetch(`${SB_URL}/rest/v1/engine_blobs?run_id=eq.${enc}&blob_type=eq.options&order=created_at.desc&limit=1`, {headers: h}),
    ]);

    const unwrap = async (r) => {
      const rows = await r.json();
      const raw = rows[0]?.content_json ?? null;
      if (!raw) return null;
      try {
        const once = typeof raw === 'string' ? JSON.parse(raw) : raw;
        return typeof once === 'string' ? JSON.parse(once) : once;
      } catch { return raw; }
    };

    const [outputBlob, stageOutputs, dPacket, f1Outputs, masterRows, optionsBlob] = await Promise.all([
      unwrap(blobRes),
      stageRes.json(),
      unwrap(dpacketRes),
      f1Res.json(),
      masterRes.json(),
      unwrap(optionsRes),
    ]);

    res.status(200).json({
      run_id,
      outputBlob,
      stageOutputs: stageOutputs || [],
      dPacket,
      f1Outputs: f1Outputs || [],
      master: masterRows?.[0] ?? null,
      optionsBlob,
      fetched_at: new Date().toISOString()
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
