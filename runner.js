const { createClient } = require('@supabase/supabase-js')
const { createHash } = require('crypto')

// ── ENV ─────────────────────────────────────────────────────
const SUPABASE_URL         = process.env.SUPABASE_URL
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_KEY
const RESEND_KEY           = process.env.RESEND_KEY
const OPENAI_KEY           = process.env.OPENAI_KEY
const ANTHROPIC_KEY        = process.env.ANTHROPIC_KEY
const GEMINI_KEY           = process.env.GEMINI_KEY
const PERPLEXITY_KEY       = process.env.PERPLEXITY_KEY

const AUDIT_WORKER_URL = 'https://tagmakes-proxy.tagmakes.workers.dev'
const SCORE_VERSION    = 'v1'
const ALL_MODELS       = ['claude', 'chatgpt', 'gemini', 'perplexity']
const BATCH_POLL_INTERVAL = 30000   // 30 seconds
const BATCH_TIMEOUT       = 1200000 // 20 minutes

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE)

function supabaseHeaders() {
  return {
    'Content-Type': 'application/json',
    'apikey': SUPABASE_SERVICE_ROLE,
    'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE}`
  }
}

// ── CONSTANTS ───────────────────────────────────────────────

// Claude system prompt - character-for-character identical to proxy for prompt caching
const CLAUDE_SYSTEM_PROMPT = `Evaluate whether the business on the provided website would be recommended by an AI assistant when answering the user's query.
Simulate the reasoning process of an AI system generating a top 3-5 business recommendation list.
Evaluate using three stages:
1. RETRIEVAL - Would this site likely appear in candidate sources for this query?
2. UNDERSTANDING - Does the site clearly explain the business, services, and location?
3. SELECTION - Would an AI confidently include this business in its top 3 recommendations?
AI RECOMMENDATION SIGNALS - score each 0-10, be honest and critical:
- readability_clarity (15%): Is the content clear and direct for AI extraction?
- structure_semantics (15%): Are headings, page structure, and semantic markup solid?
- technical_crawlability (15%): Can AI systems reliably access and parse this site?
- schema_answerability (25%): Is structured data present and complete? No schema = score 2 or lower.
- topical_authority (15%): Does the site demonstrate depth and expertise on its topic?
- entity_signals (10%): Are business name, location, NAP, and identity signals clear?
- cta_user_journey (5%): Are calls to action clear and does the page guide users?
CALIBRATION - apply strictly:
9-10 = Exceptional. Fully optimized. Rare.
7-8  = Good. Solid with minor gaps.
5-6  = Average. Exists but not optimized.
3-4  = Weak. Major gaps hurting AI visibility.
0-2  = Broken or missing. A real blocker.
ENTITY RULE - critical:
Only include businesses in entities_detected if they are real businesses you are reasonably confident exist.
If uncertain, omit the entity rather than guessing. Do not invent business names.
Return ONLY valid JSON using this exact structure:
{
  "recommended": true,
  "rank_position_estimate": 1,
  "confidence_score": 72,
  "retrieval_status": "likely",
  "understanding_status": "clear",
  "selection_status": "strong",
  "intent_interpretation": "User is looking for a local med spa in Charleston SC",
  "entities_detected": [{ "entity": "Example Business", "position": 1 }],
  "services_detected": ["med spa", "botox", "laser treatments"],
  "location_detected": "Charleston SC",
  "competitors_detected": ["Competitor A", "Competitor B"],
  "citations_or_sources": ["https://example.com"],
  "recommendation_reasoning": "The site clearly identifies services and location.",
  "signal_scores": {
    "readability_clarity": 7, "structure_semantics": 6, "technical_crawlability": 8,
    "schema_answerability": 4, "topical_authority": 6, "entity_signals": 5, "cta_user_journey": 7
  }
}
Return only JSON. No markdown. No backticks. No preamble.`

const CATEGORY_MAP = {
  "marketing agency": "Marketing Agency", "digital marketing": "Marketing Agency", "integrated marketing": "Marketing Agency", "growth marketing": "Marketing Agency", "b2b marketing": "Marketing Agency", "full service marketing": "Marketing Agency",
  "advertising agency": "Marketing Agency", "digital advertising": "Marketing Agency", "brand strategy": "Marketing Agency", "creative agency": "Marketing Agency",
  "tech pr": "Marketing Agency", "event marketing": "Marketing Agency",
  "social media": "Marketing Agency", "social media marketing": "Marketing Agency", "content strategy": "Marketing Agency", "content marketing": "Marketing Agency",
  "seo": "Marketing Agency", "local seo": "Marketing Agency", "search marketing": "Marketing Agency", "web design": "Marketing Agency",
  "graphic design": "Marketing Agency", "video marketing": "Marketing Agency", "creative production": "Marketing Agency", "brand experience": "Marketing Agency",
  "branding": "Marketing Agency", "brand consulting": "Marketing Agency", "media buying": "Marketing Agency", "ppc": "Marketing Agency",
  "dentist": "Dentist", "dental": "Dentist", "cosmetic dentist": "Dentist", "orthodontist": "Dentist", "pediatric dentist": "Dentist", "teeth whitening": "Dentist", "dental implants": "Dentist",
  "plastic surgery": "Plastic Surgery", "breast augmentation": "Plastic Surgery", "rhinoplasty": "Plastic Surgery", "facelift": "Plastic Surgery", "tummy tuck": "Plastic Surgery", "liposuction": "Plastic Surgery", "mommy makeover": "Plastic Surgery", "cosmetic surgery": "Plastic Surgery",
  "med spa": "Med Spa", "medical spa": "Med Spa", "medical aesthetics": "Med Spa", "aesthetics": "Med Spa", "botox": "Med Spa", "medspa": "Med Spa", "laser treatment": "Med Spa", "medical weight loss": "Med Spa", "dermal fillers": "Med Spa", "lip filler": "Med Spa",
  "mental health": "Healthcare", "therapist": "Healthcare", "therapy": "Healthcare", "counseling": "Healthcare", "psychiatrist": "Healthcare", "psychologist": "Healthcare",
  "healthcare": "Healthcare", "medical": "Healthcare", "chiropractor": "Healthcare", "physical therapy": "Healthcare", "optometry": "Healthcare", "urgent care": "Healthcare", "holistic health": "Healthcare", "holistic medicine": "Healthcare", "naturopath": "Healthcare", "functional medicine": "Healthcare", "womens health": "Healthcare", "women's health": "Healthcare", "gynecology": "Healthcare", "obgyn": "Healthcare", "orthopedic": "Healthcare", "hand surgery": "Healthcare",
  "law firm": "Law Firm", "attorney": "Law Firm", "legal": "Law Firm", "lawyer": "Law Firm", "divorce attorney": "Law Firm", "family law": "Law Firm", "personal injury": "Law Firm", "real estate attorney": "Law Firm", "criminal defense": "Law Firm", "estate planning": "Law Firm", "business law": "Law Firm", "immigration lawyer": "Law Firm", "dui lawyer": "Law Firm", "wrongful death": "Law Firm", "medical malpractice": "Law Firm",
  "financial": "Financial Services", "accounting": "Financial Services", "wealth management": "Financial Services", "financial advisor": "Financial Services",
  "insurance": "Financial Services", "auto insurance": "Financial Services", "home insurance": "Financial Services", "business insurance": "Financial Services", "life insurance": "Financial Services", "health insurance": "Financial Services", "commercial insurance": "Financial Services",
  "business brokerage": "Business Services", "mergers and acquisitions": "Business Services", "business valuation": "Business Services", "exit strategy": "Business Services", "franchise consulting": "Business Services", "business consulting": "Business Services",
  "real estate": "Real Estate", "realtor": "Real Estate", "property management": "Real Estate", "commercial real estate": "Real Estate",
  "architecture": "Architecture", "architect": "Architecture", "historic restoration": "Architecture", "architectural design": "Architecture", "residential architecture": "Architecture",
  "interior design": "Architecture", "residential design": "Architecture", "luxury interior design": "Architecture", "kitchen design": "Architecture",
  "wedding planning": "Event Planning", "wedding planner": "Event Planning", "wedding production": "Event Planning", "event planning": "Event Planning", "event coordinator": "Event Planning", "event design": "Event Planning", "corporate events": "Event Planning", "destination wedding": "Event Planning",
  "hotel": "Restaurant", "hospitality": "Restaurant", "tourism": "Tours & Experiences", "vacation rental": "Restaurant", "short term rental": "Restaurant", "boutique hotel": "Restaurant",
  "tour": "Tours & Experiences", "harbor tour": "Tours & Experiences", "water tour": "Tours & Experiences", "boat tour": "Tours & Experiences", "boat tours": "Tours & Experiences", "adventure tour": "Tours & Experiences",
  "restaurant": "Restaurant", "food & beverage": "Restaurant", "catering": "Restaurant", "cocktail bar": "Restaurant", "bar": "Restaurant", "coffee": "Restaurant", "bakery": "Restaurant", "cafe": "Restaurant",
  "hair color": "Med Spa", "balayage": "Med Spa", "hair extensions": "Med Spa", "salon": "Med Spa",
  "barbershop": "Wellness & Massage", "tattoo": "Wellness & Massage", "nail salon": "Wellness & Massage",
  "fitness": "Fitness & Wellness", "gym": "Fitness & Wellness", "personal training": "Fitness & Wellness", "yoga": "Fitness & Wellness",
  "massage": "Wellness & Massage", "massage therapy": "Wellness & Massage", "swedish massage": "Wellness & Massage", "deep tissue": "Wellness & Massage", "infrared sauna": "Wellness & Massage",
  "landscaping": "Home Services", "lawn care": "Home Services", "irrigation": "Home Services", "tree service": "Home Services", "hardscaping": "Home Services", "landscape design": "Home Services",
  "home services": "Home Services", "plumber": "Home Services", "plumbing": "Home Services", "hvac": "Home Services", "electrician": "Home Services", "electrical": "Home Services", "roofing": "Home Services", "pest control": "Home Services", "house cleaning": "Cleaning Services", "home cleaning": "Cleaning Services", "house organizer": "Cleaning Services", "home organizer": "Cleaning Services", "professional organizer": "Cleaning Services", "organizing": "Cleaning Services", "general contractor": "Home Services", "remodeling": "Home Services", "handyman": "Home Services", "junk removal": "Home Services", "tree removal": "Home Services",
  "limousine": "Transportation", "chauffeur": "Transportation", "chauffeured": "Transportation", "airport transfer": "Transportation", "limo": "Transportation", "charter flight": "Transportation", "shuttle": "Transportation",
  "public relations": "Public Relations", "pr agency": "Public Relations",
  "yacht charter": "Marine Services", "boat tour": "Marine Services", "marina": "Marine Services",
  "warehousing": "Business Services", "logistics": "Business Services", "3pl": "Business Services", "freight": "Business Services",
  "technology": "Business Services", "software": "Business Services", "saas": "Business Services", "it services": "Business Services",
  "retail": "Restaurant", "e-commerce": "Business Services", "boutique": "Restaurant",
}

const HOME_SERVICES_SUBINDUSTRY_MAP = {
  "plumb": "Plumbing", "drain": "Plumbing", "pipe": "Plumbing",
  "hvac": "HVAC", "heating": "HVAC", "cooling": "HVAC", "air condition": "HVAC", "furnace": "HVAC",
  "electric": "Electrical", "wiring": "Electrical", "panel": "Electrical",
  "roof": "Roofing", "gutter": "Roofing",
  "landscap": "Landscaping", "lawn": "Landscaping", "irrigation": "Landscaping",
  "pest": "Pest Control", "termite": "Pest Control", "exterminator": "Pest Control",
  "clean": "Cleaning Services", "maid": "Cleaning Services", "janitorial": "Cleaning Services",
  "contractor": "General Contractor", "remodel": "General Contractor", "renovation": "General Contractor", "handyman": "General Contractor",
}

const BREAKOUT_CATEGORIES = ['Plumbing', 'HVAC', 'Electrical', 'Roofing', 'Pest Control', 'Cleaning Services', 'General Contractor', 'Landscaping']

// ── UTILITY ─────────────────────────────────────────────────

function extractAccessCode(source) {
  if (!source) return null
  if (source.startsWith('agency_dashboard_')) return source.replace('agency_dashboard_', '')
  return null
}

function sha256(str) {
  return createHash('sha256').update(str).digest('hex')
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }

async function withConcurrency(items, limit, fn) {
  const results = []
  const executing = new Set()
  for (const item of items) {
    const p = fn(item).then(r => { executing.delete(p); return r }).catch(e => { executing.delete(p); throw e })
    executing.add(p)
    results.push(p)
    if (executing.size >= limit) await Promise.race(executing)
  }
  return Promise.allSettled(results)
}

// ── PURE FUNCTIONS (from proxy) ─────────────────────────────

function parseModelResponse(text) {
  let cleaned = text.replace(/```json[\s\S]*?```|```/gi, '').trim()
  const start = cleaned.indexOf('{')
  const end   = cleaned.lastIndexOf('}')
  if (start === -1 || end === -1) throw new Error('No JSON found in model response')
  let jsonStr = cleaned.slice(start, end + 1).replace(/\\'/g, "'").replace(/[\u0000-\u001F\u007F]/g, ' ')
  return JSON.parse(jsonStr)
}

function calculateScore(modelOutputs) {
  const models = Object.values(modelOutputs).filter(Boolean)
  const total  = models.length
  if (!total) return { finalScore: 0, recommendationProbability: 0, modelsRecommending: 0, modelsTested: 0, recommendationRate: 0, components: {} }

  const recommending       = models.filter(m => m.recommended === true).length
  const recommendationRate = recommending / total
  const positionWeights    = { 1: 1.0, 2: 0.85, 3: 0.7 }
  const positionStrength   = models.map(m => { const pos = m.rank_position_estimate; if (!pos) return 0; return positionWeights[pos] || 0.5 }).reduce((s, v) => s + v, 0) / total
  const confidenceAvg      = models.map(m => (m.confidence_score || 0) / 100).reduce((s, v) => s + v, 0) / total
  const signalDimensions   = ['readability_clarity', 'structure_semantics', 'technical_crawlability', 'schema_answerability', 'topical_authority', 'entity_signals', 'cta_user_journey']

  let signalTotal = 0, signalCount = 0
  for (const m of models) {
    if (m.signal_scores) {
      for (const dim of signalDimensions) {
        if (m.signal_scores[dim] != null) { signalTotal += m.signal_scores[dim] / 10; signalCount++ }
      }
    }
  }

  const signalAvg  = signalCount ? signalTotal / signalCount : 0.5
  const rawScore   = (0.40 * recommendationRate) + (0.20 * positionStrength) + (0.20 * confidenceAvg) + (0.20 * signalAvg)
  const finalScore = Math.round(rawScore * 100)

  return {
    finalScore,
    recommendationProbability: Math.round(recommendationRate * 100),
    modelsRecommending: recommending,
    modelsTested: total,
    recommendationRate,
    components: {
      recommendation_rate: Math.round(recommendationRate * 100),
      position_strength: Math.round(positionStrength * 100),
      confidence: Math.round(confidenceAvg * 100),
      signals: Math.round(signalAvg * 100)
    }
  }
}

function aggregateSignals(modelOutputs) {
  const dims   = ['readability_clarity', 'structure_semantics', 'technical_crawlability', 'schema_answerability', 'topical_authority', 'entity_signals', 'cta_user_journey']
  const totals = {}, counts = {}
  dims.forEach(d => { totals[d] = 0; counts[d] = 0 })
  for (const m of Object.values(modelOutputs)) {
    if (!m?.signal_scores) continue
    for (const d of dims) {
      if (m.signal_scores[d] != null) { totals[d] += m.signal_scores[d]; counts[d]++ }
    }
  }
  const result = {}
  for (const d of dims) { result[d] = counts[d] ? Math.round(totals[d] / counts[d]) : null }
  return result
}

function detectIdentity(modelOutputs, searchQuery) {
  const models = Object.values(modelOutputs).filter(Boolean)
  const pick = (field) => {
    const vals = models.map(m => m[field]).filter(Boolean)
    if (!vals.length) return null
    const freq = {}
    vals.forEach(v => { freq[v] = (freq[v] || 0) + 1 })
    return Object.entries(freq).sort((a, b) => b[1] - a[1])[0][0]
  }

  const serviceCounts = {}
  for (const m of models) {
    if (!m?.services_detected) continue
    const services = Array.isArray(m.services_detected) ? m.services_detected : [m.services_detected]
    for (const s of services) {
      if (!s) continue
      const clean = String(s).toLowerCase().trim()
      serviceCounts[clean] = (serviceCounts[clean] || 0) + 1
    }
  }
  const topService = Object.entries(serviceCounts).sort((a, b) => b[1] - a[1]).map(([s]) => s.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' '))[0] || null

  const rawIndustry = pick('intent_interpretation') || pick('services_detected') || ''
  const rawLower    = String(rawIndustry).toLowerCase()

  let mappedCategory = null
  for (const [key, val] of Object.entries(CATEGORY_MAP)) {
    if (rawLower.includes(key)) { mappedCategory = val; break }
  }
  if (!mappedCategory) {
    const reasoning = models.map(m => m.recommendation_reasoning || '').join(' ').toLowerCase()
    for (const [key, val] of Object.entries(CATEGORY_MAP)) {
      if (reasoning.includes(key)) { mappedCategory = val; break }
    }
  }

  let resolvedSubindustry = topService
  if (mappedCategory === 'Home Services') {
    const searchText = [
      searchQuery || '', rawLower,
      models.map(m => m.recommendation_reasoning || '').join(' ').toLowerCase(),
      models.map(m => (m.services_detected || []).join(' ')).join(' ').toLowerCase(),
    ].join(' ')
    for (const [keyword, subLabel] of Object.entries(HOME_SERVICES_SUBINDUSTRY_MAP)) {
      if (searchText.includes(keyword)) { mappedCategory = subLabel; resolvedSubindustry = null; break }
    }
  }
  if (BREAKOUT_CATEGORIES.includes(mappedCategory)) resolvedSubindustry = null

  return {
    businessName: null,
    businessLocation: pick('location_detected'),
    primaryService: pick('services_detected') ? JSON.stringify(pick('services_detected')) : null,
    industry: mappedCategory || null,
    subindustry: resolvedSubindustry,
    location: pick('location_detected')
  }
}

// ── PROMPT BUILDERS ─────────────────────────────────────────

function buildEvalPrompt(url, query, siteContent, schemaBlocks) {
  return `Evaluate whether the business on the provided website would be recommended by an AI assistant when answering the user's query.
Simulate the reasoning process of an AI system generating a top 3-5 business recommendation list.
Website URL: ${url}
User Query: ${query || 'general business search'}
ACTUAL WEBSITE CONTENT:
---
${siteContent}
---
SCHEMA MARKUP FOUND:
---
${schemaBlocks || 'No JSON-LD schema detected'}
---
Evaluate using three stages:
1. RETRIEVAL - Would this site likely appear in candidate sources for this query?
2. UNDERSTANDING - Does the site clearly explain the business, services, and location?
3. SELECTION - Would an AI confidently include this business in its top 3 recommendations?
AI RECOMMENDATION SIGNALS - score each 0-10, be honest and critical:
- readability_clarity (15%): Is the content clear and direct for AI extraction?
- structure_semantics (15%): Are headings, page structure, and semantic markup solid?
- technical_crawlability (15%): Can AI systems reliably access and parse this site?
- schema_answerability (25%): Is structured data present and complete? No schema = score 2 or lower.
- topical_authority (15%): Does the site demonstrate depth and expertise on its topic?
- entity_signals (10%): Are business name, location, NAP, and identity signals clear?
- cta_user_journey (5%): Are calls to action clear and does the page guide users?
CALIBRATION - apply strictly:
9-10 = Exceptional. Fully optimized. Rare.
7-8  = Good. Solid with minor gaps.
5-6  = Average. Exists but not optimized.
3-4  = Weak. Major gaps hurting AI visibility.
0-2  = Broken or missing. A real blocker.
ENTITY RULE - critical:
Only include businesses in entities_detected if they are real businesses you are reasonably confident exist.
If uncertain, omit the entity rather than guessing. Do not invent business names.
Return ONLY valid JSON using this exact structure:
{
  "recommended": true,
  "rank_position_estimate": 1,
  "confidence_score": 72,
  "retrieval_status": "likely",
  "understanding_status": "clear",
  "selection_status": "strong",
  "intent_interpretation": "User is looking for a local med spa in Charleston SC",
  "entities_detected": [{ "entity": "Example Business", "position": 1 }],
  "services_detected": ["med spa", "botox", "laser treatments"],
  "location_detected": "Charleston SC",
  "competitors_detected": ["Competitor A", "Competitor B"],
  "citations_or_sources": ["https://example.com"],
  "recommendation_reasoning": "The site clearly identifies services and location.",
  "contact_email": "info@example.com",
  "signal_scores": {
    "readability_clarity": 7, "structure_semantics": 6, "technical_crawlability": 8,
    "schema_answerability": 4, "topical_authority": 6, "entity_signals": 5, "cta_user_journey": 7
  }
}
CONTACT EMAIL RULE: Find and return the best public contact email for this business from the website content, contact page, about page, or footer. If none found, return null for contact_email.
Return only JSON. No markdown. No backticks. No preamble.`
}

function buildEvalPromptClaude(url, query, siteContent, schemaBlocks) {
  return `Website URL: ${url}
User Query: ${query || 'general business search'}
ACTUAL WEBSITE CONTENT:
---
${siteContent}
---
SCHEMA MARKUP FOUND:
---
${schemaBlocks || 'No JSON-LD schema detected'}
---`
}

// ── SITE FETCHER ────────────────────────────────────────────

async function fetchSiteContent(siteUrl) {
  let normalizedDomain = ''
  try {
    const u = siteUrl.startsWith('http') ? siteUrl : 'https://' + siteUrl
    normalizedDomain = new URL(u).hostname.replace(/^www\./, '')
  } catch (e) { normalizedDomain = siteUrl }

  let siteContent = ''
  let schemaBlocks = ''
  try {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 8000)
    const siteRes = await fetch(siteUrl.startsWith('http') ? siteUrl : 'https://' + siteUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; TaGMakesAuditBot/1.0; +https://tagmakessc.com)',
        'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8'
      },
      redirect: 'follow',
      signal: controller.signal
    })
    clearTimeout(timeout)
    const html = await siteRes.text()
    const schemaMatches = html.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi) || []
    schemaBlocks = schemaMatches.map(s => s.replace(/<\/?script[^>]*>/gi, '').trim()).join('\n')
    siteContent = html
      .replace(/<script(?![^>]*application\/ld\+json)[^>]*>[\s\S]*?<\/script>/gi, '')
      .replace(/<style[\s\S]*?<\/style>/gi, '')
      .replace(/<h([1-6])[^>]*>([\s\S]*?)<\/h\1>/gi, (_, l, c) => `[H${l}] ${c.replace(/<[^>]+>/g, '')} `)
      .replace(/<meta[^>]*name=["']description["'][^>]*content=["']([^"']+)["'][^>]*>/gi, (_, d) => `[META-DESC] ${d} `)
      .replace(/<title[^>]*>([\s\S]*?)<\/title>/gi, (_, t) => `[TITLE] ${t} `)
      .replace(/<[^>]+>/g, ' ')
      .replace(/\s+/g, ' ')
      .trim()
      .slice(0, 10000)
  } catch (e) {
    siteContent = `Could not fetch site: ${e.message}`
  }
  return { siteContent, schemaBlocks, normalizedDomain }
}

// ── REAL-TIME MODEL CALLERS ─────────────────────────────────

async function callClaudeRealtime(userPrompt, model, apiKey) {
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
    body: JSON.stringify({
      model,
      max_tokens: 2048,
      system: [{ type: 'text', text: CLAUDE_SYSTEM_PROMPT, cache_control: { type: 'ephemeral' } }],
      messages: [{ role: 'user', content: userPrompt }]
    }),
  })
  const data = await res.json()
  if (!res.ok) throw new Error('Claude error: ' + JSON.stringify(data))
  return parseModelResponse(data?.content?.[0]?.text || '')
}

async function callChatGPTRealtime(prompt, model, apiKey) {
  const res = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
    body: JSON.stringify({
      model,
      messages: [
        { role: 'system', content: 'You are an AI recommendation audit specialist. Return only valid JSON. No markdown, no backticks.' },
        { role: 'user', content: prompt }
      ]
    }),
  })
  const data = await res.json()
  if (!res.ok) throw new Error('ChatGPT error: ' + JSON.stringify(data))
  return parseModelResponse(data?.choices?.[0]?.message?.content || '')
}

async function callGeminiRealtime(prompt, apiKey) {
  const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${apiKey}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      system_instruction: { parts: [{ text: 'Return only valid JSON. No markdown, no backticks, no preamble. Start with { end with }.' }] },
      contents: [{ parts: [{ text: prompt }] }],
      generationConfig: { response_mime_type: 'application/json' }
    }),
  })
  const data = await res.json()
  if (!res.ok) throw new Error('Gemini error: ' + JSON.stringify(data))
  return parseModelResponse(data?.candidates?.[0]?.content?.parts?.[0]?.text || '')
}

async function callPerplexityRealtime(prompt, apiKey) {
  for (let attempt = 1; attempt <= 2; attempt++) {
    const res = await fetch('https://api.perplexity.ai/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
      body: JSON.stringify({
        model: 'sonar',
        messages: [
          { role: 'system', content: 'You are an AI recommendation audit specialist. You MUST return only valid JSON. Absolutely no prose, no markdown, no backticks, no explanation. Your entire response must be a single JSON object starting with { and ending with }.' },
          { role: 'user', content: prompt }
        ]
      }),
    })
    const data = await res.json()
    if (!res.ok) throw new Error('Perplexity error: ' + JSON.stringify(data))
    try {
      return parseModelResponse(data?.choices?.[0]?.message?.content || '')
    } catch (e) {
      if (attempt === 2) throw new Error('Perplexity JSON parse failed after 2 attempts: ' + e.message)
      await sleep(1000)
    }
  }
}

// ── BATCH API: OPENAI ───────────────────────────────────────

async function submitOpenAIBatch(entries, apiKey) {
  // entries: [{ customId, prompt }]
  const jsonlLines = entries.map(e => JSON.stringify({
    custom_id: e.customId,
    method: 'POST',
    url: '/v1/chat/completions',
    body: {
      model: 'gpt-4o-mini',
      messages: [
        { role: 'system', content: 'You are an AI recommendation audit specialist. Return only valid JSON. No markdown, no backticks.' },
        { role: 'user', content: e.prompt }
      ]
    }
  })).join('\n')

  // Upload JSONL file
  const blob = new Blob([jsonlLines], { type: 'application/jsonl' })
  const form = new FormData()
  form.append('file', blob, 'batch_input.jsonl')
  form.append('purpose', 'batch')
  const fileRes = await fetch('https://api.openai.com/v1/files', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${apiKey}` },
    body: form
  })
  const fileData = await fileRes.json()
  if (!fileRes.ok) throw new Error('OpenAI file upload error: ' + JSON.stringify(fileData))

  // Create batch
  const batchRes = await fetch('https://api.openai.com/v1/batches', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
    body: JSON.stringify({
      input_file_id: fileData.id,
      endpoint: '/v1/chat/completions',
      completion_window: '24h'
    })
  })
  const batchData = await batchRes.json()
  if (!batchRes.ok) throw new Error('OpenAI batch create error: ' + JSON.stringify(batchData))
  console.log(`OpenAI batch submitted: ${batchData.id} (${entries.length} entries)`)
  return batchData.id
}

async function pollOpenAIBatch(batchId, apiKey, timeoutMs = BATCH_TIMEOUT) {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    const res = await fetch(`https://api.openai.com/v1/batches/${batchId}`, {
      headers: { 'Authorization': `Bearer ${apiKey}` }
    })
    const data = await res.json()
    console.log(`OpenAI batch ${batchId}: ${data.status}`)

    if (data.status === 'completed') {
      // Download results
      const outRes = await fetch(`https://api.openai.com/v1/files/${data.output_file_id}/content`, {
        headers: { 'Authorization': `Bearer ${apiKey}` }
      })
      const outText = await outRes.text()
      const results = new Map()
      for (const line of outText.split('\n').filter(l => l.trim())) {
        try {
          const entry = JSON.parse(line)
          const content = entry?.response?.body?.choices?.[0]?.message?.content || ''
          results.set(entry.custom_id, parseModelResponse(content))
        } catch (e) {
          console.error(`OpenAI batch parse error for ${line.slice(0, 80)}: ${e.message}`)
        }
      }
      return results
    }
    if (['failed', 'expired', 'cancelled'].includes(data.status)) {
      throw new Error(`OpenAI batch ${data.status}: ${JSON.stringify(data.errors || {})}`)
    }
    await sleep(BATCH_POLL_INTERVAL)
  }
  throw new Error('OpenAI batch timeout after ' + (timeoutMs / 1000) + 's')
}

// ── BATCH API: ANTHROPIC ────────────────────────────────────

async function submitAnthropicBatch(entries, apiKey) {
  // entries: [{ customId, userPrompt }]
  const requests = entries.map(e => ({
    custom_id: e.customId,
    params: {
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 2048,
      system: [{ type: 'text', text: CLAUDE_SYSTEM_PROMPT, cache_control: { type: 'ephemeral' } }],
      messages: [{ role: 'user', content: e.userPrompt }]
    }
  }))

  const res = await fetch('https://api.anthropic.com/v1/messages/batches', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({ requests })
  })
  const data = await res.json()
  if (!res.ok) throw new Error('Anthropic batch create error: ' + JSON.stringify(data))
  console.log(`Anthropic batch submitted: ${data.id} (${entries.length} entries)`)
  return data.id
}

async function pollAnthropicBatch(batchId, apiKey, timeoutMs = BATCH_TIMEOUT) {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    const res = await fetch(`https://api.anthropic.com/v1/messages/batches/${batchId}`, {
      headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' }
    })
    const data = await res.json()
    console.log(`Anthropic batch ${batchId}: ${data.processing_status}`)

    if (data.processing_status === 'ended') {
      // Download results
      const outRes = await fetch(`https://api.anthropic.com/v1/messages/batches/${batchId}/results`, {
        headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' }
      })
      const outText = await outRes.text()
      const results = new Map()
      for (const line of outText.split('\n').filter(l => l.trim())) {
        try {
          const entry = JSON.parse(line)
          if (entry.result?.type === 'succeeded') {
            const content = entry.result.message?.content?.[0]?.text || ''
            results.set(entry.custom_id, parseModelResponse(content))
          }
        } catch (e) {
          console.error(`Anthropic batch parse error: ${e.message}`)
        }
      }
      return results
    }
    await sleep(BATCH_POLL_INTERVAL)
  }
  throw new Error('Anthropic batch timeout after ' + (timeoutMs / 1000) + 's')
}

// ── DB WRITER ───────────────────────────────────────────────

async function writeAuditResults(job, project, siteUrl, normalizedDomain, query, modelOutputs, modelVersions) {
  const scoring          = calculateScore(modelOutputs)
  const aggregatedSignals = aggregateSignals(modelOutputs)
  const identity         = detectIdentity(modelOutputs, query)
  const failedModelsList = Object.entries(modelOutputs).filter(([, v]) => !v).map(([k]) => k)

  const accessCode = extractAccessCode(job.source) || 'public'

  const auditRow = {
    project_id: job.project_id,
    website: siteUrl,
    normalized_domain: normalizedDomain,
    query: query || null,
    ai_recommendation_score: scoring.finalScore,
    recommendation_probability: scoring.recommendationProbability,
    models_recommending: scoring.modelsRecommending,
    models_total: scoring.modelsTested,
    score_components: scoring.components,
    signal_scores: aggregatedSignals,
    score_version: SCORE_VERSION,
    business_name_detected: identity.businessName,
    business_location_detected: identity.businessLocation,
    primary_service_detected: identity.primaryService,
    industry: identity.industry,
    subindustry: identity.subindustry,
    location_modifier: identity.location,
    is_public: true,
    access_code: accessCode,
    audit_environment: 'live',
    failed_models: JSON.stringify(failedModelsList),
  }

  // Write audit row
  const sbRes = await fetch(`${SUPABASE_URL}/rest/v1/audits`, {
    method: 'POST',
    headers: { ...supabaseHeaders(), 'Prefer': 'return=representation' },
    body: JSON.stringify(auditRow),
  })
  const sbText = await sbRes.text()
  if (!sbRes.ok) { console.error('Audit write failed:', sbText.slice(0, 300)); return null }

  let auditId = null
  try {
    const sbData = JSON.parse(sbText)
    const row = Array.isArray(sbData) ? sbData[0] : sbData
    auditId = row?.id || null
  } catch (e) { console.error('Audit parse failed:', e.message); return null }

  // Write model result rows
  if (auditId) {
    const modelRows = []
    for (const [modelName, parsed] of Object.entries(modelOutputs)) {
      if (!parsed) continue
      modelRows.push({
        audit_id: auditId,
        model_name: modelName,
        model_version: modelVersions[modelName] || null,
        recommended: parsed.recommended ?? null,
        rank_position_estimate: parsed.rank_position_estimate ?? null,
        confidence_score: parsed.confidence_score ?? null,
        retrieval_status: parsed.retrieval_status || null,
        understanding_status: parsed.understanding_status || null,
        selection_status: parsed.selection_status || null,
        intent_interpretation: parsed.intent_interpretation || null,
        services_detected: parsed.services_detected || null,
        location_detected: parsed.location_detected || null,
        competitors_detected: parsed.competitors_detected || null,
        citations_or_sources: parsed.citations_or_sources || null,
        signal_scores: parsed.signal_scores || null,
        recommendation_reasoning: parsed.recommendation_reasoning || null,
        raw_response_text: JSON.stringify(parsed)
      })
    }
    if (modelRows.length) {
      try {
        await fetch(`${SUPABASE_URL}/rest/v1/audit_model_results`, {
          method: 'POST',
          headers: { ...supabaseHeaders(), 'Prefer': 'return=minimal' },
          body: JSON.stringify(modelRows)
        })
      } catch (e) { console.error('Model results write error:', e.message) }
    }
  }

  // Update project location_city if detected (only if currently null)
  if (identity.businessLocation) {
    try {
      await supabase.from('projects').update({ location_city: identity.businessLocation }).eq('id', job.project_id).is('location_city', null)
    } catch (e) { /* non-fatal */ }
  }

  // Update project primary_category if detected (subindustry writing PAUSED)
  if (identity.industry) {
    try {
      await fetch(`${SUPABASE_URL}/rest/v1/projects?id=eq.${job.project_id}`, {
        method: 'PATCH',
        headers: { ...supabaseHeaders(), 'Prefer': 'return=minimal' },
        body: JSON.stringify({ primary_category: identity.industry })
      })
    } catch (e) { /* non-fatal */ }
  }

  // Write contact_email to project if found
  const contactEmail = Object.values(modelOutputs).find(m => m?.contact_email)?.contact_email || null
  if (contactEmail) {
    try {
      await supabase.from('projects').update({ contact_email: contactEmail }).eq('id', job.project_id).is('contact_email', null)
    } catch (e) { /* non-fatal */ }
  }

  return auditId
}

// ── PROCESS VIA PROXY (rerun_top10, agency, public) ─────────

async function processViaProxy(jobs) {
  console.log(`Processing ${jobs.length} jobs via proxy worker`)
  for (const job of jobs) {
    try {
      const { data: project, error: projectError } = await supabase.from('projects').select('id, domain').eq('id', job.project_id).single()
      if (projectError || !project?.domain) throw new Error(`Project lookup failed for ${job.project_id}`)

      // 24-hour requeue cap: public audits only
      const accessCode = extractAccessCode(job.source)
      const isAdminJob = (job.source || '').startsWith('requeue_') || (job.source || '').startsWith('admin_') || (job.source || '').startsWith('rerun_')
      const isPublic = !isAdminJob && (!accessCode || accessCode === 'public')
      if (isPublic) {
        const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()
        const { count: recentCount } = await supabase.from('audit_queue').select('id', { count: 'exact', head: true }).eq('project_id', job.project_id).gte('created_at', oneDayAgo)
        if ((recentCount || 0) > 2) {
          console.log(`Skipping ${project.domain}: ${recentCount} public queue entries in last 24h (cap is 2)`)
          await supabase.from('audit_queue').update({ status: 'skipped', last_error: 'Public requeue cap: >2 in 24h' }).eq('id', job.id)
          continue
        }
      }

      const siteUrl = project.domain.startsWith('http') ? project.domain : `https://${project.domain}`
      console.log(`[proxy] ${siteUrl} | source: ${job.source}`)

      const response = await fetch(AUDIT_WORKER_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ siteUrl, query: job.query, accessCode: accessCode || 'public' })
      })

      const resultText = await response.text()
      if (!response.ok) {
        let isInvalidUrl = false
        try { isInvalidUrl = JSON.parse(resultText).invalid_url === true } catch (e) {}
        if (isInvalidUrl) {
          console.log(`Invalid URL for ${siteUrl}, marking done`)
          await supabase.from('audit_queue').update({ status: 'done', processed_at: new Date().toISOString(), last_error: 'invalid_url: ' + resultText.slice(0, 500) }).eq('id', job.id)
          continue
        }
        throw new Error(`Worker error ${response.status}: ${resultText}`)
      }

      await supabase.from('audit_queue').update({ status: 'done', processed_at: new Date().toISOString(), last_error: null }).eq('id', job.id)

      // Write location back to projects
      try {
        const result = JSON.parse(resultText)
        const parsed = result.text ? JSON.parse(result.text) : result
        const location = parsed?.business_location_detected || null
        if (location) {
          await supabase.from('projects').update({ location_city: location }).eq('id', job.project_id).is('location_city', null)
        }
      } catch (e) { /* non-fatal */ }

      console.log(`[proxy] Completed: ${siteUrl}`)
    } catch (err) {
      console.error('[proxy] Audit failed:', err.message)
      const tooManyAttempts = (job.attempts || 0) >= 3
      await supabase.from('audit_queue').update({ status: tooManyAttempts ? 'failed' : 'pending', last_error: err.message }).eq('id', job.id)
    }
  }
}

// ── PROCESS DIRECT CHEAP (requeue_undermodel, low_models) ───

async function processDirectCheap(jobs) {
  console.log(`Processing ${jobs.length} jobs direct with cheap models`)

  const VERSIONS = { chatgpt: 'gpt-4o-mini', claude: 'claude-haiku-4-5-20251001', gemini: 'gemini-2.5-flash', perplexity: 'sonar' }

  await withConcurrency(jobs, 5, async (job) => {
    try {
      const { data: project, error: projectError } = await supabase.from('projects').select('id, domain').eq('id', job.project_id).single()
      if (projectError || !project?.domain) throw new Error(`Project lookup failed for ${job.project_id}`)

      const siteUrl = project.domain.startsWith('http') ? project.domain : `https://${project.domain}`
      console.log(`[cheap] ${siteUrl} | source: ${job.source}`)

      const { siteContent, schemaBlocks, normalizedDomain } = await fetchSiteContent(siteUrl)
      const evalPrompt   = buildEvalPrompt(siteUrl, job.query, siteContent, schemaBlocks)
      const claudePrompt = buildEvalPromptClaude(siteUrl, job.query, siteContent, schemaBlocks)

      const [claudeR, chatgptR, geminiR, perplexityR] = await Promise.allSettled([
        callClaudeRealtime(claudePrompt, VERSIONS.claude, ANTHROPIC_KEY),
        callChatGPTRealtime(evalPrompt, VERSIONS.chatgpt, OPENAI_KEY),
        callGeminiRealtime(evalPrompt, GEMINI_KEY),
        callPerplexityRealtime(evalPrompt, PERPLEXITY_KEY),
      ])

      const modelOutputs = {
        claude:     claudeR.status === 'fulfilled' ? claudeR.value : null,
        chatgpt:    chatgptR.status === 'fulfilled' ? chatgptR.value : null,
        gemini:     geminiR.status === 'fulfilled' ? geminiR.value : null,
        perplexity: perplexityR.status === 'fulfilled' ? perplexityR.value : null,
      }

      if (claudeR.status === 'rejected') console.error(`[cheap] Claude failed for ${siteUrl}: ${claudeR.reason?.message}`)
      if (chatgptR.status === 'rejected') console.error(`[cheap] ChatGPT failed for ${siteUrl}: ${chatgptR.reason?.message}`)
      if (geminiR.status === 'rejected') console.error(`[cheap] Gemini failed for ${siteUrl}: ${geminiR.reason?.message}`)
      if (perplexityR.status === 'rejected') console.error(`[cheap] Perplexity failed for ${siteUrl}: ${perplexityR.reason?.message}`)

      const auditId = await writeAuditResults(job, project, siteUrl, normalizedDomain, job.query, modelOutputs, VERSIONS)
      if (auditId) {
        await supabase.from('audit_queue').update({ status: 'done', processed_at: new Date().toISOString(), last_error: null }).eq('id', job.id)
        console.log(`[cheap] Completed: ${siteUrl}`)
      } else {
        throw new Error('Audit write returned no ID')
      }
    } catch (err) {
      console.error(`[cheap] Failed: ${err.message}`)
      const tooManyAttempts = (job.attempts || 0) >= 3
      await supabase.from('audit_queue').update({ status: tooManyAttempts ? 'failed' : 'pending', last_error: err.message }).eq('id', job.id)
    }
  })
}

// ── PROCESS BATCH: SERPER SEED ──────────────────────────────

async function processBatchSerperSeed(jobs) {
  console.log(`Processing ${jobs.length} serper_seed jobs via batch APIs`)

  const VERSIONS = { chatgpt: 'gpt-4o-mini', claude: 'claude-haiku-4-5-20251001', gemini: 'gemini-2.5-flash', perplexity: 'sonar' }

  // Phase 1: Fetch site content for all jobs
  const jobData = new Map()
  await withConcurrency(jobs, 10, async (job) => {
    try {
      const { data: project, error: projectError } = await supabase.from('projects').select('id, domain').eq('id', job.project_id).single()
      if (projectError || !project?.domain) throw new Error(`Project lookup failed for ${job.project_id}`)

      const siteUrl = project.domain.startsWith('http') ? project.domain : `https://${project.domain}`
      const { siteContent, schemaBlocks, normalizedDomain } = await fetchSiteContent(siteUrl)
      const evalPrompt   = buildEvalPrompt(siteUrl, job.query, siteContent, schemaBlocks)
      const claudePrompt = buildEvalPromptClaude(siteUrl, job.query, siteContent, schemaBlocks)

      jobData.set(job.id, { job, project, siteUrl, normalizedDomain, evalPrompt, claudePrompt })
    } catch (e) {
      console.error(`[batch] Site fetch failed for job ${job.id}: ${e.message}`)
      await supabase.from('audit_queue').update({ status: (job.attempts || 0) >= 3 ? 'failed' : 'pending', last_error: 'Site fetch: ' + e.message }).eq('id', job.id)
    }
  })

  if (jobData.size === 0) { console.log('[batch] No jobs with valid site content'); return }
  console.log(`[batch] Fetched ${jobData.size}/${jobs.length} sites`)

  // Phase 2: Run Gemini + Perplexity real-time
  const realtimeResults = new Map()
  await withConcurrency([...jobData.entries()], 10, async ([jobId, data]) => {
    const [geminiR, perplexityR] = await Promise.allSettled([
      callGeminiRealtime(data.evalPrompt, GEMINI_KEY),
      callPerplexityRealtime(data.evalPrompt, PERPLEXITY_KEY),
    ])
    realtimeResults.set(jobId, {
      gemini:     geminiR.status === 'fulfilled' ? geminiR.value : null,
      perplexity: perplexityR.status === 'fulfilled' ? perplexityR.value : null,
    })
    if (geminiR.status === 'rejected') console.error(`[batch] Gemini failed for ${data.siteUrl}: ${geminiR.reason?.message}`)
    if (perplexityR.status === 'rejected') console.error(`[batch] Perplexity failed for ${data.siteUrl}: ${perplexityR.reason?.message}`)
  })
  console.log(`[batch] Real-time Gemini+Perplexity done for ${realtimeResults.size} jobs`)

  // Phase 3: Submit OpenAI + Anthropic batches
  const openaiEntries = []
  const anthropicEntries = []
  for (const [jobId, data] of jobData) {
    openaiEntries.push({ customId: jobId, prompt: data.evalPrompt })
    anthropicEntries.push({ customId: jobId, userPrompt: data.claudePrompt })
  }

  let openaiResults = new Map()
  let anthropicResults = new Map()

  const [oaiBatchResult, antBatchResult] = await Promise.allSettled([
    (async () => {
      const batchId = await submitOpenAIBatch(openaiEntries, OPENAI_KEY)
      return await pollOpenAIBatch(batchId, OPENAI_KEY)
    })(),
    (async () => {
      const batchId = await submitAnthropicBatch(anthropicEntries, ANTHROPIC_KEY)
      return await pollAnthropicBatch(batchId, ANTHROPIC_KEY)
    })(),
  ])

  if (oaiBatchResult.status === 'fulfilled') {
    openaiResults = oaiBatchResult.value
    console.log(`[batch] OpenAI batch completed: ${openaiResults.size} results`)
  } else {
    console.error(`[batch] OpenAI batch failed: ${oaiBatchResult.reason?.message}`)
  }

  if (antBatchResult.status === 'fulfilled') {
    anthropicResults = antBatchResult.value
    console.log(`[batch] Anthropic batch completed: ${anthropicResults.size} results`)
  } else {
    console.error(`[batch] Anthropic batch failed: ${antBatchResult.reason?.message}`)
  }

  // Phase 4: Merge results and write to DB
  for (const [jobId, data] of jobData) {
    const rt = realtimeResults.get(jobId) || {}
    const modelOutputs = {
      chatgpt:    openaiResults.get(jobId) || null,
      claude:     anthropicResults.get(jobId) || null,
      gemini:     rt.gemini || null,
      perplexity: rt.perplexity || null,
    }

    const successCount = Object.values(modelOutputs).filter(Boolean).length
    if (successCount === 0) {
      console.error(`[batch] All models failed for ${data.siteUrl}, requeueing`)
      await supabase.from('audit_queue').update({
        status: (data.job.attempts || 0) >= 3 ? 'failed' : 'pending',
        last_error: 'All 4 models failed'
      }).eq('id', jobId)
      continue
    }

    try {
      const auditId = await writeAuditResults(data.job, data.project, data.siteUrl, data.normalizedDomain, data.job.query, modelOutputs, VERSIONS)
      if (auditId) {
        await supabase.from('audit_queue').update({ status: 'done', processed_at: new Date().toISOString(), last_error: null }).eq('id', jobId)
        console.log(`[batch] Completed: ${data.siteUrl} (${successCount}/4 models)`)
      } else {
        throw new Error('Audit write returned no ID')
      }
    } catch (err) {
      console.error(`[batch] Write failed for ${data.siteUrl}: ${err.message}`)
      await supabase.from('audit_queue').update({
        status: (data.job.attempts || 0) >= 3 ? 'failed' : 'pending',
        last_error: err.message
      }).eq('id', jobId)
    }
  }
}

// ── HEALTH CHECKS (unchanged) ───────────────────────────────

async function checkModelHealth() {
  const twoHoursAgo = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString()
  const { data, error } = await supabase.from('audit_model_results').select('model_name').gte('created_at', twoHoursAgo)
  if (error) { console.error('Model health check failed:', error.message); return }

  const activeModels = [...new Set((data || []).map(r => r.model_name))]
  const missingModels = ALL_MODELS.filter(m => !activeModels.includes(m))

  if (missingModels.length > 0) {
    console.warn(`Model health: ${activeModels.length}/4 active. Missing: ${missingModels.join(', ')}`)
    const { count } = await supabase.from('audit_queue').select('id', { count: 'exact', head: true }).eq('status', 'pending')
    if (RESEND_KEY) {
      try {
        await fetch('https://api.resend.com/emails', {
          method: 'POST',
          headers: { 'Authorization': 'Bearer ' + RESEND_KEY, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            from: 'TaG Makes <reports@tagmakessc.com>',
            to: 'therese@tagmakessc.com',
            subject: `ARO Alert: Only ${activeModels.length} of 4 models writing`,
            html: `<p>Active: ${activeModels.join(', ') || 'none'}</p><p>Missing: ${missingModels.join(', ')}</p><p>Queue: ${count || 0} pending</p><p>At: ${new Date().toISOString()}</p>`
          })
        })
      } catch (e) { console.error('Alert email failed:', e.message) }
    }
  }
  console.log('Model health: ' + activeModels.length + '/4 active')
}

async function resetStuckJobs() {
  const tenMinutesAgo = new Date(Date.now() - 10 * 60 * 1000).toISOString()
  const { data: stuck, error } = await supabase.from('audit_queue').select('id, attempts').eq('status', 'processing').lt('processed_at', tenMinutesAgo)
  if (error) { console.error('Stuck job check failed:', error.message); return }
  if (!stuck || stuck.length === 0) return

  console.log(`Resetting ${stuck.length} stuck jobs`)
  for (const job of stuck) {
    await supabase.from('audit_queue').update({
      status: 'pending',
      attempts: (job.attempts || 0) + 1,
      last_error: 'Reset: stuck in processing for >10 minutes'
    }).eq('id', job.id)
  }
}

// ── MAIN ────────────────────────────────────────────────────

async function run() {
  await resetStuckJobs()
  try { await checkModelHealth() } catch (e) { console.warn('Health check error (ignored):', e.message) }

  console.log('Claiming jobs...')
  const { data: jobs, error } = await supabase.rpc('claim_audit_queue', { batch_size: 50 })
  console.log('Claim result:', { jobsCount: jobs?.length || 0, error })
  if (error) { console.error('Claim error:', error); return }
  if (!jobs || jobs.length === 0) { console.log('No pending jobs.'); return }

  // Bucket jobs by source
  const proxyJobs = []
  const cheapJobs = []
  const batchJobs = []

  for (const job of jobs) {
    const src = (job.source || '').toLowerCase()
    if (src === 'serper_seed') {
      batchJobs.push(job)
    } else if (src.includes('undermodel') || src.includes('low_models')) {
      cheapJobs.push(job)
    } else {
      proxyJobs.push(job)
    }
  }

  console.log(`Routing: ${proxyJobs.length} proxy, ${cheapJobs.length} cheap, ${batchJobs.length} batch`)

  // Process in priority order
  if (proxyJobs.length) await processViaProxy(proxyJobs)
  if (cheapJobs.length) await processDirectCheap(cheapJobs)
  if (batchJobs.length) await processBatchSerperSeed(batchJobs)

  console.log('Run complete.')
}

run()
