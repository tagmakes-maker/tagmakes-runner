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

// Item 6: Check that all 4 AI models have written results in the last 2 hours
async function checkModelHealth() {
  const twoHoursAgo = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString()

  const { data, error } = await supabase
    .from('audit_model_results')
    .select('model_name')
    .gte('created_at', twoHoursAgo)

  if (error) {
    console.error('Model health check failed:', error.message)
    return { healthy: true } // fail open so queue isn't permanently stuck
  }

  const activeModels = [...new Set((data || []).map(r => r.model_name))]
  const missingModels = ALL_MODELS.filter(m => !activeModels.includes(m))

  if (missingModels.length > 0) {
    console.warn(`Model health check: ${activeModels.length}/4 models active. Missing: ${missingModels.join(', ')}`)

    // Get queue depth
    const { count } = await supabase
      .from('audit_queue')
      .select('id', { count: 'exact', head: true })
      .eq('status', 'pending')

    // Send alert email
    if (RESEND_KEY) {
      try {
        await fetch('https://api.resend.com/emails', {
          method: 'POST',
          headers: { 'Authorization': 'Bearer ' + RESEND_KEY, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            from: 'TaG Makes <reports@tagmakessc.com>',
            to: 'therese@tagmakessc.com',
            subject: `ARO Alert: Only ${activeModels.length} of 4 models writing - queue paused`,
            html: `<p>The audit runner detected that not all AI models are writing results.</p>
              <p><strong>Active models:</strong> ${activeModels.join(', ') || 'none'}</p>
              <p><strong>Missing models:</strong> ${missingModels.join(', ')}</p>
              <p><strong>Queue depth:</strong> ${count || 0} pending jobs</p>
              <p><strong>Checked at:</strong> ${new Date().toISOString()}</p>
              <p>The queue has been paused for this cycle. It will resume automatically when all 4 models are confirmed active.</p>`
          })
        })
        console.log('Alert email sent to therese@tagmakessc.com')
      } catch (e) {
        console.error('Failed to send alert email:', e.message)
      }
    }

    return { healthy: false, activeModels, missingModels }
  }

  console.log('Model health check: all 4 models active')
  return { healthy: true }
}

// Item 7: Reset jobs stuck in processing for more than 10 minutes
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

  console.log(`Found ${stuck.length} stuck jobs, resetting to pending...`)

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

  console.log(`Reset ${stuck.length} stuck jobs`)
}

async function run() {
  // Reset stuck jobs before claiming new ones
  await resetStuckJobs()

  // Check model health before processing
  const health = await checkModelHealth()
  if (!health.healthy) {
    console.log('Skipping batch: not all models are writing. Will retry next cycle.')
    return
  }

  console.log('Claiming jobs...')
  const { data: jobs, error } = await supabase.rpc('claim_audit_queue', { batch_size: 50 })
  console.log('Claim result:', { jobsCount: jobs?.length || 0, error })
  if (error) { console.error('Claim error:', error); return }
  if (!jobs || jobs.length === 0) { console.log('No pending jobs.'); return }

  console.log(`Processing ${jobs.length} jobs`)

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

      // 24-hour requeue cap: max 2 per domain per day (public audits only)
      const accessCode = extractAccessCode(job.source)
      const isPublic = !accessCode || accessCode === 'public'
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

      const siteUrl = project.domain.startsWith('http')
        ? project.domain
        : `https://${project.domain}`

      console.log(`Running audit for ${siteUrl} | query: ${job.query} | accessCode: ${accessCode || 'none (public)'}`)

      const response = await fetch(AUDIT_WORKER_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          siteUrl,
          query:      job.query,
          accessCode: accessCode || 'public'
        })
      })

      const resultText = await response.text()

      if (!response.ok) {
        throw new Error(`Worker error ${response.status}: ${resultText}`)
      }

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

      console.log(`Completed: ${siteUrl}`)

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
    }
  }
}

run()
