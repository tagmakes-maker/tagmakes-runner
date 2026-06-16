const { createClient } = require('@supabase/supabase-js')

const SUPABASE_URL = 'https://ttukoeysolvqimcamddq.supabase.co'
const SUPABASE_SERVICE_ROLE = 'sb_publishable_f_GNhFK4MUWETQbCCTPcMw_eMXVVmiz'
const AUDIT_WORKER_URL = 'https://tagmakes-proxy.tagmakes.workers.dev'

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE)

async function run() {
    console.log('Claiming jobs...')

    const { data: jobs, error } = await supabase.rpc('claim_audit_queue', { batch_size:  })

    console.log('Claim result:', { jobsCount: jobs?.length || 0, error })

    if (error) {
        console.error('Claim error:', error)
        return
    }

    if (!jobs || jobs.length === 0) {
        console.log('No pending jobs.')
        return
    }

    console.log(`Processing ${jobs.length} jobs`)

    for (const job of jobs) {
        try {
            const { data: project, error: projectError } = await supabase
                .from('projects')
                .select('id, domain, primary_category, subindustry, location_city')
                .eq('id', job.project_id)
                .single()

            if (projectError || !project?.domain) {
                throw new Error(`Project lookup failed for ${job.project_id}`)
            }

            const siteUrl = project.domain.startsWith('http')
                ? project.domain
                : `https://${project.domain}`

            console.log(`Running audit for ${siteUrl} | ${job.query}`)

            const response = await fetch(AUDIT_WORKER_URL, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    siteUrl,
                    query: job.query,
                    classification_source: 'queue_runner',
                    industry: project.primary_category || undefined,
                    subindustry: project.subindustry || undefined,
                    location_modifier: project.location_city || undefined
                })
            })

            const resultText = await response.text()

            if (!response.ok) {
                throw new Error(`Worker error ${response.status}: ${resultText}`)
            }

            await supabase
                .from('audit_queue')
                .update({
                    status: 'done',
                    processed_at: new Date().toISOString(),
                    last_error: null
                })
                .eq('id', job.id)

            // Write location back to projects so market_name trigger can fire
            try {
                const result = JSON.parse(resultText)
                const parsed = result.text ? JSON.parse(result.text) : result
                const location = parsed?.business_location_detected || null
                if (location) {
                    await supabase
                        .from('projects')
                        .update({ location_city: location })
                        .eq('id', job.project_id)
                        .is('location_city', null) // only update if blank
                    console.log(`Location set: ${location} → ${project.domain}`)
                }
            } catch (e) {
                console.log('Location parse skipped:', e.message)
            }

            console.log(`Completed: ${siteUrl}`)
        } catch (err) {
            console.error('Audit failed:', err.message)

            const tooManyAttempts = (job.attempts || 0) >= 3

            await supabase
                .from('audit_queue')
                .update({
                    status: tooManyAttempts ? 'failed' : 'pending',
                    last_error: err.message
                })
                .eq('id', job.id)
        }
    }
}

run()