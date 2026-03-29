const { createClient } = require('@supabase/supabase-js')

const SUPABASE_URL       = process.env.SUPABASE_URL
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_KEY
const RESEND_KEY         = process.env.RESEND_KEY
const AUDIT_WORKER_URL   = 'https://tagmakes-proxy.tagmakes.workers.dev'

const ALL_MODELS = ['claude', 'chatgpt', 'gemini', 'perplexity']

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE)

function extractAccessCode(source) {
  if (!source) return null
  if (source.startsWith('agency_dashboard_')) {
    return source.replace('agency_dashboard_', '')
  }
  return null
}

// Should this job use cheap models?
function shouldUseCheapModels(source) {
  if (!source) return false
  const src = source.toLowerCase()
  // Seed and undermodel/low_models jobs use cheap models UNLESS agency-coded
  if (src === 'serper_seed') return true
  if (src.includes('undermodel') || src.includes('low_models')) return true
  return false
}

// Check that all 4 AI models have written results in the last 2 hours
async function checkModelHealth() {
  const twoHoursAgo = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString()

  const { data, error } = await supabase
    .from('audit_model_results')
    .select('model_name')
    .gte('created_at', twoHoursAgo)

  if (error) {
    console.error('Model health check failed:', error.message)
    return
  }

  const activeModels = [...new Set((data || []).map(r => r.model_name))]
  const missingModels = ALL_MODELS.filter(m => !activeModels.includes(m))

  if (missingModels.length > 0) {
    console.warn(`Model health: ${activeModels.length}/4 active. Missing: ${missingModels.join(', ')}`)

    const { count } = await supabase
      .from('audit_queue')
      .select('id', { count: 'exact', head: true })
      .eq('status', 'pending')

    if (RESEND_KEY) {
      try {
        await fetch('https://api.resend.com/emails', {
          method: 'POST',
          headers: { 'Authorization': 'Bearer ' + RESEND_KEY, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            from: 'TaG Makes <reports@tagmakessc.com>',
            to: 'therese@tagmakessc.com',
            subject: `ARO Alert: Only ${activeModels.length} of 4 models writing`,
            html: `<p>Active: ${activeModels.join(', ') || 'none'}</p>
              <p>Missing: ${missingModels.join(', ')}</p>
              <p>Queue: ${count || 0} pending</p>
              <p>At: ${new Date().toISOString()}</p>`
          })
        })
      } catch (e) {
        console.error('Alert email failed:', e.message)
      }
    }
  }

  console.log('Model health: ' + activeModels.length + '/4 active')
}

// Reset jobs stuck in processing for more than 10 minutes
async function resetStuckJobs() {
  const tenMinutesAgo = new Date(Date.now() - 10 * 60 * 1000).toISOString()

  const { data: stuck, error } = await supabase
    .from('audit_queue')
    .select('id, attempts')
    .eq('status', 'processing')
    .lt('processed_at', tenMinutesAgo)

  if (error) {
    console.error('Stuck job check failed:', error.message)
    return
  }

  if (!stuck || stuck.length === 0) return

  console.log(`Resetting ${stuck.length} stuck jobs`)

  for (const job of stuck) {
    await supabase
      .from('audit_queue')
      .update({
        status: 'pending',
        attempts: (job.attempts || 0) + 1,
        last_error: 'Reset: stuck in processing for >10 minutes'
      })
      .eq('id', job.id)
  }
}

// Pause seed jobs and send alert email
async function pauseSeedJobs(reason) {
  if (RESEND_KEY) {
    try {
      const { count } = await supabase
        .from('audit_queue')
        .select('id', { count: 'exact', head: true })
        .eq('status', 'pending')
        .eq('source', 'serper_seed')

      await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + RESEND_KEY, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          from: 'TaG Makes <reports@tagmakessc.com>',
          to: 'therese@tagmakessc.com',
          subject: 'ARO Seed paused - model failures detected',
          html: `<p><strong>Reason:</strong> ${reason}</p>
            <p><strong>Pending seed jobs:</strong> ${count || 0}</p>
            <p><strong>At:</strong> ${new Date().toISOString()}</p>
            <p>Seed jobs will resume on next cycle once models recover.</p>`
        })
      })
      console.log('Pause alert emailed')
    } catch (e) { console.error('Pause alert email failed:', e.message) }
  }
}

async function run() {
  await resetStuckJobs()
  try { await checkModelHealth() } catch(e) { console.warn('Health check error (ignored):', e.message) }

  console.log('Claiming jobs...')
  const { data: jobs, error } = await supabase.rpc('claim_audit_queue', { batch_size: 50 })
  console.log('Claim result:', { jobsCount: jobs?.length || 0, error })
  if (error) { console.error('Claim error:', error); return }
  if (!jobs || jobs.length === 0) { console.log('No pending jobs.'); return }

  console.log(`Processing ${jobs.length} jobs`)

  let seedFailures = 0
  let seedTotal = 0
  let seedPaused = false

  for (const job of jobs) {
    try {
      const { data: project, error: projectError } = await supabase
        .from('projects')
        .select('id, domain')
        .eq('id', job.project_id)
        .single()

      if (projectError || !project?.domain) {
        throw new Error(`Project lookup failed for ${job.project_id}`)
      }

      // 24-hour requeue cap: public audits only
      const accessCode = extractAccessCode(job.source)
      const isAdminJob = (job.source || '').startsWith('requeue_') || (job.source || '').startsWith('admin_') || (job.source || '').startsWith('rerun_')
      const isPublic = !isAdminJob && (!accessCode || accessCode === 'public')
      if (isPublic) {
        const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()
        const { count: recentCount } = await supabase
          .from('audit_queue')
          .select('id', { count: 'exact', head: true })
          .eq('project_id', job.project_id)
          .gte('created_at', oneDayAgo)

        if ((recentCount || 0) > 2) {
          console.log(`Skipping ${project.domain}: ${recentCount} public queue entries in last 24h (cap is 2)`)
          await supabase
            .from('audit_queue')
            .update({ status: 'skipped', last_error: 'Public requeue cap: >2 in 24h' })
            .eq('id', job.id)
          continue
        }
      }

      // If seed jobs are paused due to failures, skip remaining seed jobs
      const isSeedJob = (job.source || '').toLowerCase() === 'serper_seed'
      if (isSeedJob && seedPaused) {
        await supabase
          .from('audit_queue')
          .update({ status: 'pending', last_error: 'Paused: model failures in this cycle' })
          .eq('id', job.id)
        continue
      }

      const siteUrl = project.domain.startsWith('http')
        ? project.domain
        : `https://${project.domain}`

      // Use cheap models for seed/undermodel jobs without agency codes
      const useCheap = !accessCode && shouldUseCheapModels(job.source)
      const tag = useCheap ? '[cheap]' : '[proxy]'

      console.log(`${tag} ${siteUrl} | source: ${job.source}`)

      const response = await fetch(AUDIT_WORKER_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          siteUrl,
          query:      job.query,
          accessCode: accessCode || 'public',
          models:     useCheap ? 'cheap' : undefined
        })
      })

      const resultText = await response.text()

      if (!response.ok) {
        let isInvalidUrl = false
        try { isInvalidUrl = JSON.parse(resultText).invalid_url === true } catch(e) {}
        if (isInvalidUrl) {
          console.log(`Invalid URL for ${siteUrl}, marking done`)
          await supabase
            .from('audit_queue')
            .update({
              status:       'done',
              processed_at: new Date().toISOString(),
              last_error:   'invalid_url: ' + resultText.slice(0, 500)
            })
            .eq('id', job.id)
          continue
        }
        throw new Error(`Worker error ${response.status}: ${resultText}`)
      }

      // Check model completeness - pause all seed jobs if any returns <4 models
      try {
        const resultCheck = JSON.parse(resultText)
        const parsedCheck = resultCheck.text ? JSON.parse(resultCheck.text) : resultCheck
        const modelsTotal = parsedCheck.models_total || 0
        const modelsFailed = parsedCheck.models_failed || []
        if (isSeedJob && modelsTotal < 4 && !seedPaused) {
          seedPaused = true
          console.warn(`${tag} Only ${modelsTotal}/4 models for ${siteUrl} (failed: ${modelsFailed.join(', ')}) - PAUSING ALL SEEDS`)
          await pauseSeedJobs(`${modelsFailed.join(', ')} failed on ${siteUrl} (${modelsTotal}/4 models)`)
        }
      } catch(e) { /* parse failed, still mark done */ }

      await supabase
        .from('audit_queue')
        .update({
          status:       'done',
          processed_at: new Date().toISOString(),
          last_error:   null
        })
        .eq('id', job.id)

      // Write location back to projects so market_name trigger fires
      try {
        const result = JSON.parse(resultText)
        const parsed = result.text ? JSON.parse(result.text) : result
        const location = parsed?.business_location_detected || null
        if (location) {
          await supabase
            .from('projects')
            .update({ location_city: location })
            .eq('id', job.project_id)
            .is('location_city', null)
          console.log(`Location set: ${location} -> ${project.domain}`)
        }
      } catch(e) {
        console.log('Location parse skipped:', e.message)
      }

      console.log(`${tag} Completed: ${siteUrl}`)

      // Track seed job success for pause logic
      if (isSeedJob) seedTotal++

    } catch (err) {
      console.error('Audit failed:', err.message)
      const tooManyAttempts = (job.attempts || 0) >= 3
      await supabase
        .from('audit_queue')
        .update({
          status:     tooManyAttempts ? 'failed' : 'pending',
          last_error: err.message
        })
        .eq('id', job.id)

      // Track seed failures for pause logic
      const isSeedJob = (job.source || '').toLowerCase() === 'serper_seed'
      if (isSeedJob) {
        seedTotal++
        seedFailures++
        // If >50% of seed jobs are failing, pause the rest
        if (seedTotal >= 3 && seedFailures / seedTotal > 0.5 && !seedPaused) {
          seedPaused = true
          console.warn(`Seed pause triggered: ${seedFailures}/${seedTotal} failures`)
          await pauseSeedJobs(`${seedFailures}/${seedTotal} seed jobs failed this cycle`)
        }
      }
    }
  }

  // Run automated trial email checks
  try {
    console.log('Checking trial emails...')
    const runnerKey = SUPABASE_SERVICE_ROLE.slice(-8)
    const trialRes = await fetch(AUDIT_WORKER_URL + '/admin/check-trials?key=' + encodeURIComponent(runnerKey))
    const trialData = await trialRes.json()
    if (trialData.midSent || trialData.endSent) {
      console.log(`Trial emails: ${trialData.midSent} mid-trial, ${trialData.endSent} trial-end sent (${trialData.checked} checked)`)
    }
  } catch(e) { console.warn('Trial check failed (non-fatal):', e.message) }
}

run()
