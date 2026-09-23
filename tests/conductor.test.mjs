import test from 'node:test';
import assert from 'node:assert/strict';
import {Conductor,MockProvider,EventBus} from '../src/conductor.mjs';
const input=(id='demo')=>({id,scope:'demo-project',operation:'synthetic.render',estimatedCents:5});
const approve=(c,id='demo')=>c.approve(id,{reviewer:'demo-reviewer',scope:'demo-project',fingerprint:c.job(id).fingerprint});
test('approval gates dispatch and binds scope plus immutable proposal',async()=>{const p=new MockProvider(),c=new Conductor({provider:p});c.prepare(input());await assert.rejects(c.run('demo'),/Approval/);assert.equal(p.calls,0);assert.throws(()=>c.approve('demo',{reviewer:'x',scope:'wrong',fingerprint:c.job('demo').fingerprint}),/binding/);assert.equal(c.ledger().length,0);approve(c);assert.equal((await c.run('demo')).state,'succeeded');assert.equal(c.ledger()[0].actualCents,3);});
test('same idempotency key with different content is rejected',()=>{const c=new Conductor();c.prepare(input());assert.equal(c.prepare(input()).state,'waiting_approval');assert.throws(()=>c.prepare({...input(),estimatedCents:9}),/conflict/);});
test('concurrent duplicate calls dispatch exactly once within this process',async()=>{const p=new MockProvider(),c=new Conductor({provider:p});c.prepare(input());approve(c);await Promise.all([c.run('demo'),c.run('demo'),c.run('demo')]);assert.equal(p.calls,1);});
test('UNKNOWN holds reservation and is never automatically retried',async()=>{const p=new MockProvider([{throws:true}]),c=new Conductor({provider:p,budgetCents:5});c.prepare(input());approve(c);assert.equal((await c.run('demo')).state,'unknown');await c.run('demo');assert.equal(p.calls,1);assert.equal(c.ledger()[0].kind,'reserved');c.prepare(input('second'));assert.throws(()=>approve(c,'second'),/Budget/);});
test('reconciliation requires bound evidence and cannot charge twice',async()=>{const c=new Conductor({provider:new MockProvider([{status:'unknown'}])});c.prepare(input());approve(c);await c.run('demo');const e={scope:'demo-project',fingerprint:c.job('demo').fingerprint,status:'succeeded',costCents:4,evidenceRef:'receipt-1'};assert.throws(()=>c.reconcile('demo',{...e,scope:'other'}),/binding/);assert.equal(c.reconcile('demo',e).state,'succeeded');assert.equal(c.ledger()[0].actualCents,4);assert.throws(()=>c.reconcile('demo',e),/UNKNOWN/);});
test('retry policy only retries known non-acceptance and is bounded',async()=>{const p=new MockProvider([{status:'not_accepted',costCents:0,retryable:true}]),c=new Conductor({provider:p,maxAttempts:2});c.prepare(input());approve(c);assert.equal((await c.run('demo')).state,'failed');assert.equal(p.calls,2);assert.equal(c.ledger()[0].actualCents,0);});
test('retryable flag does not make an ambiguous response safe to retry',async()=>{const p=new MockProvider([{status:'failed',costCents:0,retryable:true}]),c=new Conductor({provider:p});c.prepare(input());approve(c);assert.equal((await c.run('demo')).state,'unknown');assert.equal(p.calls,1);});
test('untrusted non-integer costs remain UNKNOWN and do not corrupt ledger',async()=>{const c=new Conductor({provider:new MockProvider([{status:'succeeded',costCents:NaN}])});c.prepare(input());approve(c);assert.equal((await c.run('demo')).state,'unknown');assert.equal(c.ledger()[0].reservedCents,5);});
test('actual overruns are retained as evidence, not clamped away',async()=>{const c=new Conductor({provider:new MockProvider([{status:'succeeded',costCents:9}]),budgetCents:5});c.prepare(input());approve(c);await c.run('demo');assert.equal(c.ledger()[0].actualCents,9);assert.ok(c.events().some(e=>e.type==='cost.overrun'));});
test('snapshots cannot mutate audit trail or proposals',()=>{const c=new Conductor();c.prepare(input());c.job('demo').estimatedCents=0;c.events().pop();assert.equal(c.job('demo').estimatedCents,5);assert.equal(c.events().length,2);});
test('negative, fractional and extra fields are rejected',()=>{const c=new Conductor();for(const estimatedCents of [-1,.5,Infinity])assert.throws(()=>c.prepare({...input(),estimatedCents}));assert.throws(()=>c.prepare({...input(),apiKey:'synthetic-not-a-key'}),/fields/);});
test('cancelled proposal cannot execute',async()=>{const p=new MockProvider(),c=new Conductor({provider:p});c.prepare(input());c.cancel('demo');assert.equal((await c.run('demo')).state,'cancelled');assert.equal(p.calls,0);});

test('event bus preserves order, filters subscriptions and isolates failures',async()=>{
 const bus=new EventBus(), seen=[]; const c=new Conductor({eventBus:bus});
 bus.subscribe('*',e=>{seen.push(e.sequence);e.type='mutated';});
 let stateEvents=0;const unsub=bus.subscribe('state.changed',()=>{stateEvents++;throw new Error('synthetic');});
 c.prepare(input());approve(c);await c.run('demo');
 const report=await bus.drain();assert.deepEqual(seen,[1,2,3,4,5,6]);
 assert.equal(stateEvents,4);assert.equal(report.failures.length,4);
 assert.equal(c.events()[0].type,'job.prepared');unsub();
 assert.equal((await bus.drain()).deliveredEvents,0);
});
test('events published by subscribers wait for the next drain',async()=>{
 const bus=new EventBus(), seen=[];bus.subscribe('first',()=>bus.publish({type:'second',sequence:2}));
 bus.subscribe('*',e=>seen.push(e.type));bus.publish({type:'first',sequence:1});
 await bus.drain();assert.deepEqual(seen,['first']);await bus.drain();assert.deepEqual(seen,['first','second']);
});
