// Updated: October 1, 2026 (forward accessCode parsed from job.source to the audit worker, ClickUp wdvrdayqg7 -- previously every audit created here landed access_code="public" and never showed up in any agency's /console; distinct from the existing source:job.source passthrough below, which the worker does not read as accessCode). Previous entry: July 14, 2026 (credit/quota circuit breaker)
const { createClient } = require('@supabase/supabase-js')

const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_KEY
const AUDIT_WORKER_URL = 'https://tagmakes-proxy.tagmakes.workers.dev'
const RESEND_KEY = process.env.RESEND_KEY
const ALERT_EMAIL = 'therese@tagmakessc.com'
const ALERT_FROM = 'TaG Makes <reports@tagmakessc.com>'

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE)

const AI_CRAWLERS = ['GPTBot', 'ClaudeBot', 'anthropic-ai', 'Google-Extended', 'PerplexityBot', 'CCBot']

// Per-model API error strings get wrapped as "<Model> error: " + JSON.stringify(errorBody)
// by tagmakes-proxy-worker.js's callClaude/callChatGPT/callGemini/callPerplexity. Note the
// worker still returns HTTP 200 for a v2 audit even when one or more models fail (per-model
// errors are absorbed via Promise.allSettled into a `modelErrors` field) -- so credit/quota
// exhaustion has to be detected by scanning response text, not just non-2xx status.
const CREDIT_EXHAUSTION_PATTERNS = {
    claude: /credit balance is too low/i,
    chatgpt: /insufficient_quota/i,
    gemini: /RESOURCE_EXHAUSTED|billing account/i,
    perplexity: /quota/i
}

function detectCreditExhaustion(text) {
    if (!text) return null
    for (const [model, pattern] of Object.entries(CREDIT_EXHAUSTION_PATTERNS)) {
        if (pattern.test(text)) return model
    }
    return null
}

function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]))
}

async function sendCreditAlertEmail(model, errorText) {
    if (!RESEND_KEY) {
        console.error(`RESEND_KEY not set -- could not send credit alert email for ${model}`)
        return
    }
    try {
        const res = await fetch('https://api.resend.com/emails', {
            method: 'POST',
            headers: { 'Authorization': 'Bearer ' + RESEND_KEY, 'Content-Type': 'application/json' },
            body: JSON.stringify({
                from: ALERT_FROM,
                to: ALERT_EMAIL,
                subject: `AUDIT QUEUE PAUSED: ${model} out of credits`,
                html: `<p>The audit runner detected a billing/quota-exhausted error from <strong>${model}</strong> and paused the audit queue (all 'pending' rows set to 'paused').</p><pre style="white-space:pre-wrap">${escapeHtml(errorText || '')}</pre>`
            })
        })
        if (!res.ok) {
            console.error('Credit alert email failed:', await res.text())
        }
    } catch (e) {
        console.error('Credit alert email error:', e.message)
    }
}

// Pauses all pending queue rows and sends one alert email. Skips the email (but still logs)
// if there were zero pending rows to pause -- that means an earlier trip this outage already
// paused the queue, so this isn't a new incident.
async function tripCreditBreaker(model, errorText) {
    console.error(`CIRCUIT BREAKER TRIPPED: ${model} credit/quota exhausted -- ${errorText}`)

    const { count: pendingCount, error: countError } = await supabase
        .from('audit_queue')
        .select('id', { count: 'exact', head: true })
        .eq('status', 'pending')

    if (countError) {
        console.error('Circuit breaker: failed to count pending rows:', countError)
    }

    if (!pendingCount) {
        console.log('Circuit breaker: no pending rows to pause -- queue already paused from an earlier trip this outage, skipping duplicate alert email')
        return
    }

    const note = `CIRCUIT BREAKER: ${model} credit/quota exhausted - ${(errorText || '').slice(0, 500)}`

    const { error: pauseError } = await supabase
        .from('audit_queue')
        .update({ status: 'paused', last_error: note })
        .eq('status', 'pending')

    if (pauseError) {
        console.error('Circuit breaker: failed to pause pending rows:', pauseError)
    } else {
        console.log(`Circuit breaker: paused ${pendingCount} pending row(s)`)
    }

    await sendCreditAlertEmail(model, errorText)
}

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
        let breakerTripped = false
        let breakerModel = null
        let breakerErrorText = null

        try {
            const { data: project, error: projectError } = await supabase
                .from('projects')
                .select('id, domain, primary_category, subindustry')
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

            // No location_modifier sent -- the proxy worker's own project-record lookup
            // (market_name + location_state, with Haiku fallback and distrust logic for
            // unverified records) is the single source of truth for location resolution.
            // A runner-supplied bare city with no state was short-circuiting that lookup
            // and always failing the worker's clean "City ST" shape check.

            // Forward the agency code from the queue row's source (e.g. agency_trial_FULLGALLOP_20261001
            // or agency_dashboard_TAGMAKES2026) as accessCode -- the worker's main audit route reads
            // body.accessCode specifically (not body.source), and defaults to access_code="public" when
            // it's absent. Without this, every audit this runner creates for an agency job is invisible
            // in that agency's /console (ClickUp wdvrdayqg7).
            const sourceCodeMatch = (job.source || '').match(/^agency_(?:trial|dashboard)_([A-Z0-9]+)/i)
            const accessCode = sourceCodeMatch ? sourceCodeMatch[1].toUpperCase() : undefined

            const response = await fetch(AUDIT_WORKER_URL, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    siteUrl,
                    query: job.query,
                    source: job.source,
                    classification_source: 'queue_runner',
                    industry: project.primary_category || undefined,
                    subindustry: project.subindustry || undefined,
                    accessCode
                })
            })

            const resultText = await response.text()

            const embeddedCreditModel = detectCreditExhaustion(resultText)
            if (embeddedCreditModel) {
                breakerTripped = true
                breakerModel = embeddedCreditModel
                breakerErrorText = resultText
            }

            if (!response.ok) {
                throw new Error(`Worker error ${response.status}: ${resultText}`)
            }

            // 2xx is not proof an audit row was written: needsLocation/needsBank/rateLimited
            // are 200s with no insert. Do not mark these done.
            try {
                const peek = JSON.parse(resultText)
                if (peek && (peek.needsLocation || peek.needsBank || peek.rateLimited)) {
                    throw new Error(`Worker returned no-audit response (200): ${resultText.slice(0, 300)}`)
                }
            } catch (e) {
                if (e.message.startsWith('Worker returned no-audit')) throw e
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

            if (!breakerTripped) {
                const caughtCreditModel = detectCreditExhaustion(err.message)
                if (caughtCreditModel) {
                    breakerTripped = true
                    breakerModel = caughtCreditModel
                    breakerErrorText = err.message
                }
            }

            if (breakerTripped) {
                await supabase
                    .from('audit_queue')
                    .update({
                        status: 'paused',
                        last_error: `CIRCUIT BREAKER: ${breakerModel} credit/quota exhausted - ${err.message}`.slice(0, 2000)
                    })
                    .eq('id', job.id)
            } else {
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

        if (breakerTripped) {
            await tripCreditBreaker(breakerModel, breakerErrorText)
            console.log('Circuit breaker tripped -- halting remaining jobs in this batch.')
            break
        }
    }
}

run()