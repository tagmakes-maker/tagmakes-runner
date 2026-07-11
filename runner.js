// Updated: July 10, 2026 (pre-check: GET redirect follow, bot_blocked, robots_blocks_ai)
const { createClient } = require('@supabase/supabase-js')

const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_KEY
const AUDIT_WORKER_URL = 'https://tagmakes-proxy.tagmakes.workers.dev'

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE)

const AI_CRAWLERS = ['GPTBot', 'ClaudeBot', 'anthropic-ai', 'Google-Extended', 'PerplexityBot', 'CCBot']

function checkRobotsForAiAgents(txt) {
    const lines = txt.split(/\r?\n/).map(l => l.split('#')[0].trim())
    let inAiBlock = false
    for (const line of lines) {
        const lower = line.toLowerCase()
        if (lower.startsWith('user-agent:')) {
            const ua = line.slice(11).trim()
            inAiBlock = AI_CRAWLERS.some(a => a.toLowerCase() === ua.toLowerCase())
        } else if (inAiBlock && lower.startsWith('disallow:')) {
            if (line.slice(9).trim() === '/') return true
        } else if (line === '') {
            inAiBlock = false
        }
    }
    return false
}

async function preCheck(siteUrl) {
    const result = { finalUrl: siteUrl, botBlocked: null, robotsBlocksAi: null }
    try {
        let currentUrl = siteUrl
        let res = null
        for (let hops = 0; hops <= 3; hops++) {
            const controller = new AbortController()
            const timer = setTimeout(() => controller.abort(), 10000)
            let r
            try {
                r = await fetch(currentUrl, { method: 'GET', redirect: 'manual', signal: controller.signal })
            } finally {
                clearTimeout(timer)
            }
            if (r.status >= 300 && r.status < 400 && hops < 3) {
                const loc = r.headers.get('location')
                if (!loc) { res = r; break }
                currentUrl = new URL(loc, currentUrl).href
            } else {
                res = r; break
            }
        }
        result.finalUrl = currentUrl
        if (res) {
            if (res.status === 403) {
                result.botBlocked = true
            } else if (res.status === 200) {
                const ct = (res.headers.get('content-type') || '').toLowerCase()
                if (ct.includes('text/html')) {
                    const body = await res.text()
                    if (/cloudflare ray id|just a moment\.\.\.|captcha|cf-browser-verification/i.test(body)) {
                        result.botBlocked = true
                    } else {
                        result.botBlocked = false
                    }
                } else {
                    result.botBlocked = false
                }
            } else {
                result.botBlocked = false
            }
        }
    } catch (e) {
        console.log(`Pre-check error for ${siteUrl}: ${e.message}`)
    }
    try {
        const origin = new URL(result.finalUrl).origin
        const rRes = await fetch(`${origin}/robots.txt`, { method: 'GET' })
        if (rRes.ok) {
            result.robotsBlocksAi = checkRobotsForAiAgents(await rRes.text())
        } else {
            result.robotsBlocksAi = false
        }
    } catch (e) {
        // robots.txt unreachable — leave null
    }
    return result
}

async function run() {
    console.log('Claiming jobs...')

    const { data: jobs, error } = await supabase.rpc('claim_audit_queue', { batch_size: 50 })

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

            const preCheckResult = await preCheck(siteUrl)
            if (new URL(preCheckResult.finalUrl).hostname !== new URL(siteUrl).hostname) {
                console.log(`Redirect: ${new URL(siteUrl).hostname} -> ${new URL(preCheckResult.finalUrl).hostname} (not overwriting project)`)
            }

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

            // Write pre-check flags to most recent audit for this project
            try {
                const { data: latestAudit } = await supabase
                    .from('audits')
                    .select('id')
                    .eq('project_id', job.project_id)
                    .order('created_at', { ascending: false })
                    .limit(1)
                    .single()
                if (latestAudit?.id) {
                    const flagUpdate = { robots_blocks_ai: preCheckResult.robotsBlocksAi }
                    if (preCheckResult.botBlocked !== null) flagUpdate.bot_blocked = preCheckResult.botBlocked
                    await supabase.from('audits').update(flagUpdate).eq('id', latestAudit.id)
                    console.log(`Pre-check flags: bot_blocked=${preCheckResult.botBlocked}, robots_blocks_ai=${preCheckResult.robotsBlocksAi} (${new URL(preCheckResult.finalUrl).hostname})`)
                }
            } catch (e) {
                console.log('Pre-check flag write failed:', e.message)
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