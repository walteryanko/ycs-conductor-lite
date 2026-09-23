import { createHash } from 'node:crypto';

// New offline reference implementation of the audited production architecture.
// No production handlers, credentials, provider routing or persistence are copied.
const canonical = value => Array.isArray(value) ? `[${value.map(canonical).join(',')}]`
  : value && typeof value === 'object' ? `{${Object.keys(value).sort().map(k => `${JSON.stringify(k)}:${canonical(value[k])}`).join(',')}}`
  : JSON.stringify(value);
const digest = value => createHash('sha256').update(canonical(value)).digest('hex');
const transitions = {
  prepared: ['waiting_approval'], waiting_approval: ['authorized', 'cancelled'],
  authorized: ['running'], running: ['succeeded', 'failed', 'unknown'],
  unknown: ['succeeded', 'failed'], succeeded: [], failed: [], cancelled: []
};
function requireThat(ok, message) { if (!ok) throw new Error(message); }
function cents(n) { requireThat(Number.isSafeInteger(n) && n >= 0, 'Invalid integer cost'); return n; }
function safeId(s) { requireThat(typeof s === 'string' && /^[a-zA-Z0-9_-]{1,80}$/.test(s), 'Invalid identifier'); return s; }
function exact(value, keys) { requireThat(value && Object.getPrototypeOf(value) === Object.prototype && Object.keys(value).every(k => keys.includes(k)), 'Invalid fields'); }

export class MockProvider {
  calls = 0;
  constructor(outcomes = [{status:'succeeded', costCents:3}]) { this.outcomes = structuredClone(outcomes); }
  async execute() { const index=this.calls++; const value=this.outcomes[Math.min(index,this.outcomes.length-1)]; if(value?.throws) throw new Error('Ambiguous transport failure'); return structuredClone(value); }
}

// Pull-driven event delivery keeps handlers outside state transitions.
// Subscribers are trusted host code. Delivery is in-memory and at most once.
export class EventBus {
  #queue=[]; #subscriptions=new Set(); #draining=false;
  subscribe(type, handler) {
    requireThat(typeof type==='string' && typeof handler==='function','Invalid subscription');
    const sub={type,handler};this.#subscriptions.add(sub);
    return ()=>this.#subscriptions.delete(sub);
  }
  publish(event) { this.#queue.push(structuredClone(event)); }
  async drain() {
    requireThat(!this.#draining,'Drain already running');this.#draining=true;
    const failures=[],batch=this.#queue.splice(0);
    try { for(const event of batch) for(const sub of [...this.#subscriptions]) {
      if(sub.type!=='*' && sub.type!==event.type)continue;
      try { await sub.handler(structuredClone(event)); }
      catch { failures.push({sequence:event.sequence,type:event.type,reason:'SUBSCRIBER_FAILED'}); }
    }} finally { this.#draining=false; }
    return {deliveredEvents:batch.length,failures};
  }
}

export class Conductor {
  #jobs = new Map(); #events = []; #ledger = []; #provider;
  #budget; #maxAttempts; #bus;
  constructor({provider = new MockProvider(), budgetCents = 100, maxAttempts = 2, eventBus = new EventBus()} = {}) {
    this.#bus=eventBus;
    this.#provider=provider; this.#budget=cents(budgetCents);
    requireThat(Number.isInteger(maxAttempts) && maxAttempts>=1 && maxAttempts<=5,'Invalid retry bound');
    this.#maxAttempts=maxAttempts;
  }
  #emit(type, job, extra={}) { const event=Object.freeze({sequence:this.#events.length+1,type,jobId:job.id,...extra});this.#events.push(event);this.#bus.publish(event); }
  #transition(job, next) { requireThat(transitions[job.state]?.includes(next),'Invalid state transition'); job.state=next; this.#emit('state.changed',job,{state:next}); }
  #get(id) { const job=this.#jobs.get(id); requireThat(job,'Job not found'); return job; }
  #committedCost() { return this.#ledger.reduce((sum,row)=>sum+(row.kind==='settled'?row.actualCents:row.reservedCents),0); }
  #cost(job) { return this.#ledger.find(row=>row.jobId===job.id); }
  prepare(input) {
    exact(input,['id','scope','operation','estimatedCents']);
    safeId(input.id);safeId(input.scope);cents(input.estimatedCents);
    requireThat(input.operation==='synthetic.render','Unsupported operation');
    const fingerprint=digest(input), existing=this.#jobs.get(input.id);
    if(existing) { requireThat(existing.fingerprint===fingerprint,'Idempotency conflict'); return this.job(input.id); }
    const job={...structuredClone(input),fingerprint,state:'prepared',attempts:0};
    this.#jobs.set(job.id,job);this.#emit('job.prepared',job);this.#transition(job,'waiting_approval');
    return this.job(job.id);
  }
  approve(id, {reviewer, scope, fingerprint}) {
    const job=this.#get(id);safeId(reviewer);
    requireThat(scope===job.scope && fingerprint===job.fingerprint,'Approval binding mismatch');
    requireThat(job.state==='waiting_approval','Job not awaiting approval');
    requireThat(this.#committedCost()+job.estimatedCents<=this.#budget,'Budget exceeded');
    this.#ledger.push({jobId:id,kind:'reserved',reservedCents:job.estimatedCents});
    job.reviewer=reviewer;this.#transition(job,'authorized');return this.job(id);
  }
  cancel(id) { const job=this.#get(id);this.#transition(job,'cancelled');return this.job(id); }
  #settle(job, status, cost) {
    cents(cost);const row=this.#cost(job); row.kind='settled';row.actualCents=cost;
    this.#transition(job,status);
    if(cost>job.estimatedCents) this.#emit('cost.overrun',job,{estimatedCents:job.estimatedCents,actualCents:cost});
  }
  async run(id) {
    const job=this.#get(id);
    if(['running','succeeded','failed','unknown','cancelled'].includes(job.state))return this.job(id);
    requireThat(job.state==='authorized','Approval required');
    // Move before awaiting: concurrent calls in this process cannot dispatch twice.
    this.#transition(job,'running');
    while(job.attempts<this.#maxAttempts) {
      job.attempts++;this.#emit('provider.attempt',job,{attempt:job.attempts});
      let result;
      try { result=await this.#provider.execute({idempotencyKey:job.fingerprint,operation:job.operation}); }
      catch { this.#transition(job,'unknown');return this.job(id); }
      const validCost=Number.isSafeInteger(result?.costCents)&&result.costCents>=0;
      if(result?.status==='succeeded' && validCost) { this.#settle(job,'succeeded',result.costCents);return this.job(id); }
      // Retry only a provider-confirmed rejection BEFORE acceptance, with known zero cost.
      if(result?.status==='not_accepted' && result.costCents===0) {
        if(result.retryable===true && job.attempts<this.#maxAttempts) {this.#emit('retry.scheduled',job,{attempt:job.attempts+1});continue;}
        this.#settle(job,'failed',0);return this.job(id);
      }
      this.#transition(job,'unknown');return this.job(id);
    }
    throw new Error('Unreachable retry state');
  }
  reconcile(id, evidence) {
    const job=this.#get(id);
    requireThat(job.state==='unknown','Only UNKNOWN can be reconciled');
    exact(evidence,['scope','fingerprint','status','costCents','evidenceRef']);
    requireThat(evidence.scope===job.scope && evidence.fingerprint===job.fingerprint,'Evidence binding mismatch');
    safeId(evidence.evidenceRef);requireThat(['succeeded','failed'].includes(evidence.status),'Unresolved evidence');
    cents(evidence.costCents);this.#emit('result.reconciled',job,{evidenceRef:evidence.evidenceRef});
    this.#settle(job,evidence.status,evidence.costCents);return this.job(id);
  }
  job(id) { return structuredClone(this.#get(id)); }
  events() { return structuredClone(this.#events); }
  ledger() { return structuredClone(this.#ledger); }
}
