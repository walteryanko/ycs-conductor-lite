import {Conductor,MockProvider} from '../src/conductor.mjs';
const c=new Conductor({provider:new MockProvider([{status:'unknown'}]),budgetCents:10});
const job=c.prepare({id:'demo',scope:'synthetic-project',operation:'synthetic.render',estimatedCents:5});
c.approve(job.id,{scope:job.scope,fingerprint:job.fingerprint,reviewer:'demo-reviewer'});
await c.run(job.id);
console.log('Ambiguous response:',c.job(job.id).state,'reserved:',c.ledger());
c.reconcile(job.id,{scope:job.scope,fingerprint:job.fingerprint,status:'succeeded',costCents:3,evidenceRef:'synthetic-receipt'});
console.log(JSON.stringify({job:c.job(job.id),ledger:c.ledger(),events:c.events()},null,2));
