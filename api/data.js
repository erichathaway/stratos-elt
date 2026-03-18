export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  let { run_id } = req.query;
  if (!run_id) {
    // No run_id supplied — resolve latest completed run from engine_master_state
    const latestRes = await fetch(`${SB_URL}/rest/v1/engine_master_state?order=created_at.desc&limit=1`, {headers: h});
    const latestRows = await latestRes.json();
    run_id = latestRows?.[0]?.run_id;
    if (!run_id) { res.status(404).json({ error: 'No runs found in engine_master_state' }); return; }
  }

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
    const [blobRes, stageRes, dpacketRes, f1Res, masterRes, optionsRes, govRes] = await Promise.all([
      fetch(`${SB_URL}/rest/v1/engine_blobs?run_id=eq.${enc}&blob_type=eq.OUTPUT&order=created_at.desc&limit=1`, {headers: h}),
      fetch(`${SB_URL}/rest/v1/engine_stage_outputs?run_id=eq.${enc}&section=eq.D&order=round.asc,role_key.asc`, {headers: h}),
      fetch(`${SB_URL}/rest/v1/engine_blobs?run_id=eq.${enc}&blob_type=eq.D_packet&order=round.desc&limit=1`, {headers: h}),
      fetch(`${SB_URL}/rest/v1/engine_stage_outputs?run_id=eq.${enc}&section=eq.F1&order=round.asc`, {headers: h}),
      fetch(`${SB_URL}/rest/v1/engine_master_state?run_id=eq.${enc}&limit=1`, {headers: h}),
      fetch(`${SB_URL}/rest/v1/engine_blobs?run_id=eq.${enc}&blob_type=eq.options&order=created_at.desc&limit=1`, {headers: h}),
      fetch(`${SB_URL}/rest/v1/engine_blobs?run_id=eq.${enc}&blob_type=eq.GOV&order=created_at.desc&limit=1`, {headers: h}),
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

    const [outputBlob, stageOutputs, dPacket, f1Outputs, masterRows, optionsBlob, govBlob] = await Promise.all([
      unwrap(blobRes),
      stageRes.json(),
      unwrap(dpacketRes),
      f1Res.json(),
      masterRes.json(),
      unwrap(optionsRes),
      unwrap(govRes),
    ]);

    // Transform Drive webViewLinks to direct-stream URLs for audio
    function driveAudioUrl(link) {
      if (!link) return null;
      const m = link.match(/\/d\/([a-zA-Z0-9_-]+)\//);
      return m ? `https://drive.google.com/uc?export=download&id=${m[1]}` : link;
    }
    function driveEmbedUrl(link) {
      if (!link) return null;
      const m = link.match(/\/d\/([a-zA-Z0-9_-]+)\//);
      return m ? `https://drive.google.com/file/d/${m[1]}/preview` : link;
    }
    if (govBlob) {
      govBlob.audio_brief_url = driveAudioUrl(govBlob.audio_brief_url);
      govBlob.audio_deep_url  = driveAudioUrl(govBlob.audio_deep_url);
      govBlob.brief_html_url  = driveEmbedUrl(govBlob.brief_html_url);
      if (govBlob.walkthrough_sections) {
        govBlob.walkthrough_sections = govBlob.walkthrough_sections.map(s => ({
          ...s,
          audio_url: driveAudioUrl(s.view_link)
        }));
      }
    }

    res.status(200).json({
      run_id,
      outputBlob,
      stageOutputs: stageOutputs || [],
      dPacket,
      f1Outputs: f1Outputs || [],
      master: masterRows?.[0] ?? null,
      optionsBlob,
      govBlob,
      fetched_at: new Date().toISOString()
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

// WRITE_TEST_MARKER