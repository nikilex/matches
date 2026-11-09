import { test } from 'node:test';
import assert from 'node:assert/strict';
import { WebSocket } from 'ws';
import { createApp } from '../server.js';

test('rooms, hidden draws, concurrent selection, reconnect and reset', async t => {
  const {server,wss}=createApp();
  await new Promise(r=>server.listen(0,'127.0.0.1',r));
  const base=`http://127.0.0.1:${server.address().port}`;
  const clients=[];
  t.after(async()=>{for(const c of clients)c.ws.terminate();for(const ws of wss.clients)ws.terminate();await new Promise(r=>server.close(r));});
  assert.equal((await fetch(base)).status,200);
  assert.equal((await fetch(`${base}/../server.js`)).status,404);
  async function client(){
    const ws=new WebSocket(base.replace('http','ws')); const messages=[]; const waiters=[];
    ws.on('message',raw=>{const m=JSON.parse(raw);messages.push(m);for(const f of [...waiters])f();});
    await new Promise(r=>ws.on('open',r));
    const c={ws,send:m=>ws.send(JSON.stringify(m)),next:(predicate)=>new Promise((resolve,reject)=>{const timer=setTimeout(()=>{cleanup();reject(Error('Timed out waiting for message'));},3000);const cleanup=()=>{clearTimeout(timer);const i=waiters.indexOf(check);if(i>=0)waiters.splice(i,1);};function check(){const i=messages.findIndex(predicate);if(i>=0){const m=messages.splice(i,1)[0];cleanup();resolve(m);}}waiters.push(check);check();})};clients.push(c);return c;
  }
  const a=await client();a.send({type:'create',name:'Анна',count:2,broken:1});
  const joined=await a.next(m=>m.type==='joined');
  const first=(await a.next(m=>m.type==='state')).room;
  assert.ok(first.matches.every(m=>!('broken' in m)));
  const b=await client();b.send({type:'join',name:'Борис',code:joined.code});await b.next(m=>m.type==='joined');await b.next(m=>m.type==='state');
  const outsider=await client();outsider.send({type:'join',name:'Лишний',code:joined.code});assert.match((await outsider.next(m=>m.type==='error')).message,/достаточно/);
  a.send({type:'pick',index:0});b.send({type:'pick',index:0});
  // Exactly one concurrent draw succeeds, the other receives a conflict.
  const after=(await a.next(m=>m.type==='state'&&m.room.matches[0].owner)).room;
  assert.equal(after.matches.filter(m=>m.owner).length,1);
  assert.equal('broken' in after.matches[1],false);
  const winner=after.matches[0].owner===after.you?a:b, loser=winner===a?b:a;
  assert.match((await loser.next(m=>m.type==='error')).message,/забрали/);
  loser.send({type:'reset'});assert.equal((await loser.next(m=>m.type==='error')).type,'error');
  loser.send({type:'pick',index:1});
  const complete=(await a.next(m=>m.type==='state'&&m.room.complete)).room;
  assert.equal(complete.matches.filter(m=>m.broken).length,1);
  a.send({type:'pick',index:1});assert.match((await a.next(m=>m.type==='error')).message,/уже вытянули/);
  a.ws.close();await new Promise(r=>a.ws.on('close',r));
  const back=await client();back.send({type:'join',name:'Анна',code:joined.code,token:joined.token});await back.next(m=>m.type==='joined');
  const restored=(await back.next(m=>m.type==='state')).room;assert.equal(restored.you,first.you);assert.equal(restored.players.length,2);
  back.send({type:'reset'});const next=(await back.next(m=>m.type==='state'&&m.room.round===2)).room;
  assert.ok(next.players.every(p=>p.pick===null));assert.ok(next.matches.every(m=>!m.owner&&!('broken'in m)));
});
