// BLOCK BATTLE 1.4 online test server – single process, in-memory rooms.
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const {WebSocketServer,WebSocket}=require('ws');
const PORT=process.env.PORT||3000,formats={'1v1':2,'1v1v1':3,'2v2':4,'3v3':6,'royale5':5,'royale10':10};
const queue=new Map(Object.keys(formats).map(f=>[f,[]])),rooms=new Map(),clients=new Set(),leaderboard=new Map();
const files={'/':'index.html','/index.html':'index.html','/icon.svg':'icon.svg','/manifest.webmanifest':'manifest.webmanifest'};
const server=http.createServer((req,res)=>{let pathname;try{pathname=new URL(req.url,'http://localhost').pathname}catch{return res.writeHead(400).end()}
 if(pathname==='/health'){res.writeHead(200,{'content-type':'application/json'});return res.end(JSON.stringify({ok:true,players:clients.size,rooms:rooms.size}))}
 const file=files[pathname];if(!file){res.writeHead(404).end('Not found');return}
 fs.readFile(path.join(__dirname,file),(err,buf)=>{if(err){res.writeHead(404).end();return}res.writeHead(200,{'content-type':file.endsWith('.html')?'text/html; charset=utf-8':file.endsWith('.svg')?'image/svg+xml':'application/manifest+json','cache-control':'no-store','x-content-type-options':'nosniff'});res.end(buf)})});
const wss=new WebSocketServer({server,path:'/ws',maxPayload:4096});
function send(c,m){if(c.ws.readyState===WebSocket.OPEN)c.ws.send(JSON.stringify(m))}
function roster(room){return room.members.map(c=>({id:c.id,name:c.name,country:c.country,score:c.score,eliminated:!!c.eliminated}))}
function publish(room,m){for(const c of room.members)send(c,m)}
function code(){let x;do{x=crypto.randomBytes(5).toString('hex').slice(0,6).toUpperCase()}while(rooms.has(x));return x}
function dequeue(c){for(const q of queue.values()){let i=q.indexOf(c);if(i!==-1)q.splice(i,1)}c.searching=null}
function leave(c){dequeue(c);const room=c.room;if(!room)return;if(room.started&&isRoyale(room)&&!room.ended){c.eliminated=true;publish(room,{type:'eliminated',id:c.id});}c.room=null;room.members=room.members.filter(p=>p!==c);publish(room,{type:'left',id:c.id});if(room.members.length){publish(room,{type:'roster',players:roster(room)})}else{clearTimeout(room.timer);rooms.delete(room.code)}if(room.started&&!room.ended)checkWinner(room)}
function isRoyale(room){return room.format==='royale5'||room.format==='royale10'}
function closeRoom(room){if(room.ended)return;room.ended=true;clearTimeout(room.timer);setTimeout(()=>{if(rooms.get(room.code)===room){publish(room,{type:'closed'});for(const p of room.members)p.room=null;rooms.delete(room.code)}},15000)}
function checkWinner(room){if(!isRoyale(room)||!room.started||room.ended)return;const active=room.members.filter(c=>!c.eliminated);if(active.length<=1){const winner=active[0];publish(room,{type:'royaleWinner',id:winner?.id||null,name:winner?.name||'Ingen'});closeRoom(room)}}
function enter(c,room){if(room.started||room.members.length>=formats[room.format])return send(c,{type:'error',message:'Kampen er allerede startet eller fuld'});leave(c);c.score=0;c.eliminated=false;c.room=room;room.members.push(c);publish(room,{type:'room',code:room.code,format:room.format,players:roster(room)});if(room.members.length===formats[room.format]){room.started=true;room.start=Date.now()+5000;publish(room,{type:'start',start:room.start,players:roster(room)});room.timer=setTimeout(()=>{if(isRoyale(room)){const winner=room.members.filter(c=>!c.eliminated).sort((a,b)=>b.score-a.score)[0];publish(room,{type:'royaleWinner',id:winner?.id||null,name:winner?.name||'Ingen'});}else publish(room,{type:'ended',players:roster(room)});closeRoom(room)},isRoyale(room)?605000:185000)}}
function processQueue(f){const q=queue.get(f);q.splice(0,q.length,...q.filter(c=>c.ws.readyState===WebSocket.OPEN&&!c.room&&c.searching===f));while(q.length>=formats[f]){const members=q.splice(0,formats[f]);const room={code:code(),format:f,members:[],started:false,ended:false};rooms.set(room.code,room);for(const c of members){c.searching=null;enter(c,room)}}for(const c of q)send(c,{type:'searching',count:q.length,target:formats[f]})}
wss.on('connection',(ws,req)=>{const c={ws,id:crypto.randomUUID(),name:'Spiller',country:'DK',room:null,searching:null,score:0,lastScoreAt:0};clients.add(c);ws.on('message',raw=>{let m;try{m=JSON.parse(raw.toString())}catch{return}if(!m||typeof m.type!=='string')return;
 if(m.type==='hello'){if(typeof m.id==='string'&&/^[0-9a-f-]{36}$/i.test(m.id)&&![...clients].some(p=>p!==c&&p.id===m.id))c.id=m.id;c.name=String(m.name||'Spiller').slice(0,28).replace(/[<>]/g,'');c.country=String(m.country||'DK').slice(0,3);send(c,{type:'hello'});return}
 if(m.type==='cancel'){dequeue(c);return}
 if(m.type==='leave'){leave(c);return}
 if(m.type==='search'){if(!formats[m.format])return send(c,{type:'error',message:'Ugyldig kampform'});leave(c);c.searching=m.format;queue.get(m.format).push(c);processQueue(m.format);return}
 if(m.type==='create'){if(!formats[m.format])return;leave(c);const key=code(),room={code:key,format:m.format,members:[],started:false,ended:false};rooms.set(key,room);enter(c,room);return}
 if(m.type==='join'){const room=rooms.get(String(m.code||'').toUpperCase());if(!room)return send(c,{type:'error',message:'Kampkoden blev ikke fundet'});enter(c,room);return}
 if(m.type==='chat'&&c.room&&!c.room.ended){
  const now=Date.now();if(now-(c.lastChatAt||0)<1200)return;
  const message=String(m.text||'').trim().slice(0,160).replace(/[\u0000-\u001f\u007f]/g,'');
  if(!message)return;c.lastChatAt=now;
  publish(c.room,{type:'chat',code:c.room.code,name:c.name,text:message});return;
 }
 if(m.type==='eliminated'&&c.room&&isRoyale(c.room)&&c.room.started&&!c.room.ended&&!c.eliminated){c.eliminated=true;publish(c.room,{type:'eliminated',id:c.id});checkWinner(c.room);return}
 if(m.type==='power'&&c.room&&isRoyale(c.room)&&c.room.started&&!c.room.ended&&!c.eliminated){
  const now=Date.now();if(now<(c.nextAttack||0))return;
  if(!['freeze','slip'].includes(m.kind))return;
  // Testprotokol: serveren begrænser hyppighed, men validerer endnu ikke optjente BP.
  c.nextAttack=now+12000;
  const targets=c.room.members.filter(p=>p!==c&&!p.eliminated);
  if(!targets.length)return;
  const target=targets[crypto.randomInt(targets.length)];
  send(target,{type:'power',kind:m.kind,from:c.id});return;
 }
 if(m.type==='ranking'){
  const name=String(m.name||c.name||'Spiller').trim().slice(0,28);
  const country=String(m.country||c.country||'').slice(0,32);
  const xp=Math.max(0,Math.min(10000000,Math.floor(Number(m.xp)||0)));
  const wins=Math.max(0,Math.min(1000000,Math.floor(Number(m.wins)||0)));
  const played=Math.max(0,Math.min(1000000,Math.floor(Number(m.played)||0)));
  // Beta only: player-reported data is not authoritative.
  leaderboard.set(c.id,{id:c.id,name,country,xp,wins,played});
  return;
 }
 if(m.type==='leaderboard'){
  const players=[...leaderboard.values()].sort((a,b)=>b.xp-a.xp||b.wins-a.wins).slice(0,100);
  return send(c,{type:'leaderboard',players});
 }
 if(m.type==='score'&&c.room&&c.room.started&&!c.room.ended){const now=Date.now();if(now-c.lastScoreAt<400)return;c.lastScoreAt=now;const n=Number(m.score);if(!Number.isFinite(n)||n<0||n>100000000)return;c.score=Math.max(c.score,Math.floor(n));publish(c.room,{type:'score',id:c.id,score:c.score})}
 });ws.on('close',()=>{const f=c.searching;leave(c);clients.delete(c);if(f)processQueue(f)});ws.on('error',()=>{})});
server.listen(PORT,'0.0.0.0',()=>console.log('BLOCK BATTLE online test: http://localhost:'+PORT));
