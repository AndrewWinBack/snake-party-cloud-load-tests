import http from 'k6/http';
import {WebSocket} from 'k6/websockets';
import {SharedArray} from 'k6/data';
import {Counter,Rate,Trend} from 'k6/metrics';
import exec from 'k6/execution';
import {sleep,check} from 'k6';
import {randomBytes,sha256} from 'k6/crypto';
const fixture=new SharedArray('fixture',()=>[JSON.parse(open('../runtime/fixture.json'))])[0];
if(![60,180].includes(fixture.duration)||fixture.pps!==5)throw new Error('Invalid Snake workload');
if(__ENV.GITHUB_ACTIONS!=='true')throw new Error('This acceptance harness only runs on GitHub-hosted cloud runners');
const shard=Number(__ENV.SHARD_INDEX),shards=4,N=fixture.players,nodeN=N/shards,indexOffset=shard*nodeN;
if(!Number.isInteger(shard)||shard<0||shard>=shards||!Number.isInteger(nodeN)||fixture.minimumSources!==4)throw new Error('Invalid cloud shard');
const workers=Math.min(20,nodeN),batch=Math.ceil(nodeN/workers),groups=Math.ceil(nodeN/batch),runSeconds=fixture.duration+600;
const path=fixture.base+'/api/rooms/'+fixture.code,wsURL=path.replace(/^http/,'ws')+'/ws';
const plannedCount=Array.from({length:nodeN},(_,i)=>i+indexOffset).filter(i=>i%5===0).length;
const errors=new Rate('game_errors'),scoreMatch=new Rate('score_matches'),stateMatch=new Rate('recovered_state_matches'),deadlineRejected=new Rate('deadline_rejected');
const ackMs=new Trend('score_ack_ms',true),readyMs=new Trend('player_ready_ms',true),joinMs=new Trend('player_join_ms',true),gaps=new Trend('snapshot_gap_ms',true),timerLag=new Trend('generator_timer_lag_ms',true),heartbeatMs=new Trend('heartbeat_rtt_ms',true),recoveryMs=new Trend('planned_recovery_ms',true);
const joined=new Counter('players_joined'),readyCount=new Counter('players_ready'),finished=new Counter('players_finished'),ticks=new Counter('ticks_accepted'),reconnections=new Counter('planned_reconnections'),unexpectedClose=new Counter('unexpected_disconnects'),recovered=new Counter('planned_reconnections_recovered'),scoreUpdates=new Counter('score_updates'),checkpoints=new Counter('board_checkpoints'),ackTimeouts=new Counter('ack_timeouts'),joinRetries=new Counter('join_retries'),lateChecks=new Counter('late_submissions_rejected'),collisions=new Counter('collisions_verified');
const rateLimited=new Counter('rate_limited_429'),wireBytes=new Counter('player_message_bytes'),playerSnapshots=new Counter('player_snapshots');
const serverQueue=new Trend('server_queue_ms',true),serverApply=new Trend('server_apply_ms',true),serverCommit=new Trend('server_commit_ms',true);
const zone='github-cloud-'+shard;
const expected=(index,seq)=>seq?fixture.trajectories[index%fixture.trajectories.length][seq-1]:{score:0,foods:0,deaths:0,hash:fixture.initialHash};
function correct(index,seq,score,foods){const e=expected(index,seq);return !!e&&e.score===score&&e.foods===foods&&score===foods*10+Math.floor(foods/5)*30}
// Protocol 3 intentionally skips unchanged player snapshots. Cadence SLA applies to
// live host/display updates; player health is validated by ACKs, heartbeats and finish.
export const options={
 scenarios:{players:{executor:'per-vu-iterations',vus:groups,iterations:1,maxDuration:runSeconds+'s',exec:'players'},controller:{executor:'per-vu-iterations',vus:1,iterations:1,maxDuration:runSeconds+'s',exec:'cloudController'},display:{executor:'per-vu-iterations',vus:1,iterations:1,maxDuration:runSeconds+'s',exec:'display'}},
 thresholds:{rate_limited_429:['count==0'],game_errors:['rate==0'],score_updates:['count>0'],board_checkpoints:['count>0'],score_matches:['rate==1'],recovered_state_matches:['rate==1'],deadline_rejected:['rate==1'],late_submissions_rejected:['count=='+nodeN],score_ack_ms:['p(95)<500','p(99)<1000'],player_ready_ms:['p(95)<3000'],player_join_ms:['p(95)<2000'],snapshot_gap_ms:['p(95)<2500'],generator_timer_lag_ms:['p(99)<200'],players_joined:['count=='+nodeN],players_ready:['count=='+(nodeN+plannedCount)],players_finished:['count=='+nodeN],planned_reconnections:['count=='+plannedCount],planned_reconnections_recovered:['count=='+plannedCount],planned_recovery_ms:['p(95)<8000'],ticks_accepted:['count>='+Math.floor(nodeN*fixture.duration*5*.9)],checks:['rate==1'],http_req_failed:['rate<0.01'],unexpected_disconnects:['count==0'],ack_timeouts:['count==0'],collisions_verified:['count>0']},
 systemTags:['status','method','name','group','check','error','error_code','scenario'],summaryTrendStats:['avg','med','p(90)','p(95)','p(99)','max']
};
if(shard!==0){delete options.scenarios.controller;delete options.scenarios.display;delete options.thresholds.snapshot_gap_ms;}
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function required(value,label){if(!check(value,{[label]:v=>!!v}))throw new Error(label)}
export async function cloudController(){
 const key=__ENV.TEST_HOST_KEY;if(!key)throw new Error('Missing disposable room key');
 let ws,beat,retry,stopping=false,hostReady=false,hostSnapshots=0,lastSnapshot=0,hostBoardPlayers=0;
 function connect(){if(stopping)return;const current=new WebSocket(wsURL);ws=current;current.onopen=()=>current.send(JSON.stringify({type:'auth',role:'host',key,protocol:3}));current.onmessage=e=>{if(current!==ws||e.data==='pong')return;const m=JSON.parse(e.data);if(m.type==='ready'||m.type==='snapshot'){if(m.room.status==='playing'&&lastSnapshot)gaps.add(Date.now()-lastSnapshot,{role:'host'});lastSnapshot=m.room.status==='playing'?Date.now():0;hostReady=true;hostSnapshots++;hostBoardPlayers=m.players.length}};current.onclose=e=>{if(current!==ws||stopping)return;hostReady=false;unexpectedClose.add(1,{role:'host',code:String(e.code)});console.warn(JSON.stringify({event:'unexpected_close',role:'host',code:e.code}));retry=setTimeout(connect,1000)}}
 async function host(body,suffix=''){const r=await http.asyncRequest(body?'POST':'GET',path+suffix,body?JSON.stringify(body):null,{headers:{Authorization:'Bearer '+key,'Content-Type':'application/json'},timeout:'15s',tags:{name:suffix?'host diagnostics':'cloud host action'}});if(r.status!==200)throw new Error('Host HTTP '+r.status);return r.json()}
 try{
  connect();beat=setInterval(()=>{if(ws?.readyState===1)ws.send('ping')},4000);let started=false;const deadline=Date.now()+480000;
  while(Date.now()<deadline){const s=await host();if(hostReady&&s.total===N&&s.connectedPlayers===N){started=true;break}await delay(1000)}
  required(started,'all player sockets connected before shared start');
  const initial=await host(null,'/network');console.log(JSON.stringify({event:'cloud_network',stage:'initial',network:initial}));required(initial.players===N&&initial.uniqueSources>=fixture.minimumSources,'cloud source provenance observed');
  const {room}=await host({action:'start',round:1});required(room.endsAt-room.startsAt===fixture.duration*1000,'exact requested duration');console.log(JSON.stringify({event:'cloud_started',room:fixture.code,players:N,startsAt:room.startsAt,endsAt:room.endsAt}));
  while(Date.now()<room.endsAt+2500)await delay(500);
  const final=await host(),ranks=final.players;required(final.room.status==='finished'&&ranks.length===N,'all players in final result');
  required(ranks.every(p=>p.seq>0&&correct(Number(p.name.slice(5)),p.seq,p.score,p.foods)),'every authoritative score and bonus verified');
  const rankOK=ranks.every((p,i)=>{if(!i)return p.rank===1;const prev=ranks[i-1],tied=prev.score===p.score&&prev.foods===p.foods;return(prev.score>p.score||prev.score===p.score&&prev.foods>=p.foods)&&p.rank===(tied?prev.rank:i+1)});required(rankOK,'complete ranking and ties verified');
  const minTicks=Math.floor((fixture.duration-8)*5*.9);required(ranks.every(p=>p.seq>=minTicks),'every player sustained movement workload');
  required(hostSnapshots>0&&hostBoardPlayers===N,'host received full leaderboard');
  const history=await host(null,'/history?round=1');required(history.players?.length===N&&history.players.every((p,i)=>['id','name','score','foods','seq','alive','rank'].every(k=>p[k]===ranks[i][k])),'archived results equal final leaderboard');
  const network=await host(null,'/network');required(network.storageAudit?.players===N&&network.storageAudit.mismatches===0,'durable stored boards equal authoritative state');console.log(JSON.stringify({event:'cloud_network',stage:'final',network}));
  console.log(JSON.stringify({event:'cloud_verification',ok:true,room:fixture.code,players:N,verifiedMatchSeconds:(room.endsAt-room.startsAt)/1000,startsAt:room.startsAt,endsAt:room.endsAt,hostSnapshots,hostBoardPlayers,ticks:ranks.reduce((n,p)=>n+p.seq,0),minTicks:Math.min(...ranks.map(p=>p.seq)),maxTicks:Math.max(...ranks.map(p=>p.seq)),scoreSum:ranks.reduce((n,p)=>n+p.score,0),foodSum:ranks.reduce((n,p)=>n+p.foods,0),allScoresVerified:true,rankingVerified:rankOK,historyVerified:true,allPlayersSustained:true}));
 }catch(e){console.error(JSON.stringify({event:'cloud_control_failed',message:String(e.message)}));exec.test.abort('Cloud controller verification failed')}
 finally{stopping=true;clearInterval(beat);clearTimeout(retry);ws?.close(1000)}
}
export function display(){
 let ws,last=0,beat,closing=false;const deadline=setTimeout(()=>{errors.add(true);closing=true;ws?.close()},runSeconds*1000-1000);
 ws=new WebSocket(wsURL);ws.onopen=()=>{ws.send(JSON.stringify({type:'auth',role:'display',protocol:3}));beat=setInterval(()=>{if(ws.readyState===1)ws.send('ping')},4000)};
 ws.onmessage=e=>{if(e.data==='pong')return;const m=JSON.parse(e.data);if(m.type==='ready'||m.type==='snapshot'){if(m.room.status==='playing'&&last)gaps.add(Date.now()-last,{role:'display'});last=m.room.status==='playing'?Date.now():0;if(m.room.status==='finished'){check(m,{'display receives final three snake boards':s=>s.spectators?.length===Math.min(3,N)&&s.spectators.every(p=>p.game.snake?.length>=4&&correct(Number(p.name.slice(5)),p.game.seq,p.game.score,p.game.foods))});closing=true;ws.close(1000)}}};
 ws.onclose=e=>{clearInterval(beat);clearTimeout(deadline);if(!closing){unexpectedClose.add(1,{role:'display',code:String(e.code)});console.warn(JSON.stringify({event:'unexpected_close',role:'display',code:e.code}))}};
}
export function players(){
 rateLimited.add(0);errors.add(false);scoreMatch.add(true);stateMatch.add(true);unexpectedClose.add(0);ackTimeouts.add(0);checkpoints.add(0);scoreUpdates.add(0);collisions.add(0);joinRetries.add(0);
 const group=exec.scenario.iterationInTest;console.log(JSON.stringify({event:'cloud_generator',zone,instance:shard,group,players:Math.min(batch,nodeN-group*batch)}));if(fixture.joinRampSeconds)sleep(group*batch/nodeN*fixture.joinRampSeconds);
 for(let j=0;j<batch;j++){
  const index=group*batch+j+indexOffset;if(index>=indexOffset+nodeN)break;const at=Date.now(),joinKey=Array.from(new Uint8Array(randomBytes(32)),v=>v.toString(16).padStart(2,'0')).join('');let r;
  for(let attempt=0;attempt<3;attempt++){r=http.post(path,JSON.stringify({action:'join',name:'LOAD-'+String(index).padStart(5,'0'),joinKey}),{headers:{'Content-Type':'application/json'},timeout:'8s',tags:{name:'player join'}});if(r.status===429)rateLimited.add(1);if(r.status===200||r.status>0&&r.status<500&&![408,429].includes(r.status)||attempt===2)break;joinRetries.add(1);sleep(attempt===0?.5:1.5)}
  joinMs.add(Date.now()-at);if(r.status!==200){errors.add(true);console.warn(JSON.stringify({event:'join_failed',player:index,status:r.status,error:r.error}));continue}joined.add(1);startPlayer(index,r.json().key,r.json().wsPath)
 }
}
function startPlayer(index,key,wsPath){
 if(wsPath!==undefined&&!(typeof wsPath==='string'&&new RegExp('^/api/rooms/'+fixture.code+'/ws\\?shard=[0-3]$').test(wsPath)))throw new Error('Invalid same-room player route');
 const playerWsURL=wsPath?fixture.base.replace(/^http/,'ws')+wsPath:wsURL;
 let room=null,confirmed=0,localSeq=0,sealedSeq=0,localScore=0,unreported=[],queue=[],pending=null,ws,timer,beat,offset=0,pingAt=0,lastSnapshot=0,syncError=false,retryAt=0,reconnected=false,recoveringAt=0,done=false,finalChecked=false,errorLogs=0,clockSkew=0,connectionSerial=0;
 const deadline=setTimeout(()=>{errors.add(true);done=true;ws?.close();clearInterval(timer);clearInterval(beat)},runSeconds*1000-1500);
 const logError=(event,data)=>{if(errorLogs++<5)console.warn(JSON.stringify({event,player:index,...data}))};
 function connect(){
  const opened=Date.now(),serial=++connectionSerial,connection=new WebSocket(playerWsURL,[],{tags:{player:String(index)}});ws=connection;let intentional=false,lastMessageAt=opened,lastMessageType='connecting';
  connection.onopen=()=>connection.send(JSON.stringify({type:'auth',role:'player',key,protocol:3}));
  connection.onmessage=e=>{
   if(ws!==connection||done)return;const now=Date.now();lastMessageAt=now;if(e.data==='pong'){if(pingAt){heartbeatMs.add(now-pingAt);pingAt=0}return}wireBytes.add(e.data.length,{zone});const m=JSON.parse(e.data);lastMessageType=m.type;
   if(m.type==='ready'){
    offset=m.serverNow-now;const game=m.me.game;check(game,{'player receives authoritative board':g=>Number.isInteger(g.seq)&&Array.isArray(g.snake)});scoreMatch.add(correct(index,game.seq,game.score,game.foods));stateMatch.add(sha256(JSON.stringify(game),'hex')===expected(index,game.seq)?.hash);
    if(recoveringAt){recovered.add(1);recoveryMs.add(now-recoveringAt);recoveringAt=0;lastSnapshot=0}
    readyMs.add(now-opened);readyCount.add(1);confirmed=localSeq=sealedSeq=game.seq;localScore=game.score;unreported=[];queue=[];pending=null;syncError=false;retryAt=0;
    clearInterval(timer);clearInterval(beat);let due=now+200;timer=setInterval(()=>{const now=Date.now();timerLag.add(Math.max(0,now-due));due=now+200;play()},200);beat=setInterval(()=>{if(ws.readyState===1){pingAt=Date.now();ws.send('ping')}},4000);
   }
   if(m.type==='ready'||m.type==='snapshot'){
    playerSnapshots.add(1,{zone});lastSnapshot=now;room=m.room;clockSkew=m.serverNow-now-offset;
    if(room.status==='finished'&&!finalChecked){
     finalChecked=true;finished.add(1);scoreMatch.add(correct(index,m.me.seq,m.me.score,m.me.foods));clearInterval(timer);
     ws.send(JSON.stringify({type:'checkpoint',requestId:'late',round:1,fromSeq:m.me.seq,seq:m.me.seq+1,pieces:[expected(index,m.me.seq+1).actions]}));
     setTimeout(()=>{if(!done){errors.add(true);logError('late_check_timeout',{});stop()}},5000);
    }
   }
   if(m.type==='ack'&&pending&&m.requestId===pending.id){
    ackMs.add(now-pending.at,{zone});if(m.serverTiming){serverQueue.add(m.serverTiming.queue);serverApply.add(m.serverTiming.apply);serverCommit.add(m.serverTiming.commit);}errors.add(false);ticks.add(m.game.seq-confirmed);scoreMatch.add(correct(index,m.game.seq,m.game.score,m.game.foods));
    const deaths=expected(index,m.game.seq).deaths-expected(index,confirmed).deaths;if(deaths)collisions.add(deaths);
    if(pending.payload.type==='score')scoreUpdates.add(1);else checkpoints.add(1);confirmed=m.game.seq;queue.shift();pending=null;syncError=false;retryAt=0;flush();
   }
   if(m.type==='error'){
    if(m.status===429)rateLimited.add(1);
    if(m.requestId==='late'){deadlineRejected.add(m.status===409);if(m.status===409)lateChecks.add(1);stop();return}
    if(m.status===409&&m.error==='本場不在計分時間'&&room&&now+offset>=room.endsAt-1000){pending=null;queue=[];return}
    errors.add(true);logError('game_error',{status:m.status,error:m.error,sequence:confirmed,pendingSequence:pending?.payload.seq,offset,clockSkew,elapsed:room?now+offset-room.startsAt:null});pending=null;syncError=true;retryAt=now+1000;
   }
  };
  connection.onerror=e=>{errors.add(true);logError('socket_error',{message:String(e.error||e.message||e.type),phase:room?.status})};
  connection.onclose=e=>{console.log(JSON.stringify({event:'socket_closed',player:index,serial,at:Date.now(),age:Date.now()-opened,code:e.code??null,reason:e.reason??'',eventKeys:Object.keys(e),state:connection.readyState,current:ws===connection,intentional,done,lastMessageAge:Date.now()-lastMessageAt,lastMessageType,phase:room?.status,sequence:confirmed,endsAt:room?.endsAt,serverTime:Date.now()+offset}));if(ws!==connection)return;clearInterval(timer);clearInterval(beat);if(!done&&!intentional){unexpectedClose.add(1,{role:'player',code:String(e.code)});logError('unexpected_close',{code:e.code??null,wasClean:e.wasClean??null,eventType:e.type??null,reason:e.reason,phase:room?.status,sequence:confirmed});setTimeout(connect,1000)}};
  function play(){
   const now=Date.now(),serverTime=now+offset;if(done||!room||room.status==='lobby'||room.status==='finished'||serverTime<room.startsAt||serverTime>=room.endsAt||ws.readyState!==1)return;
   if(!reconnected&&index%5===0&&serverTime>room.startsAt+fixture.duration*500){reconnected=true;intentional=true;recoveringAt=now;reconnections.add(1);ws.close(1000,'planned reconnect');setTimeout(connect,5000);return}
   if(pending&&now-pending.at>5000){ackTimeouts.add(1);errors.add(true);logError('ack_timeout',{sequence:confirmed,pendingSequence:pending.payload.seq});intentional=true;ws.close(1000,'ack timeout recovery');setTimeout(connect,500);return}
   // Match GamePlay: one step per 200 ms timer, no catch-up bursts; pause on rejected updates.
   if(syncError){if(now>=retryAt){retryAt=now+1000;flush()}return}
   if(queue.length>=3)return;
   if(localSeq+1>Math.floor((serverTime-room.startsAt)/fixture.tickMs))return;
   const move=expected(index,localSeq+1);if(!move)return;const gained=move.score>localScore;localSeq++;localScore=move.score;unreported.push(move.actions);
   if(gained||unreported.length>=5){queue.push({type:gained?'score':'checkpoint',round:1,fromSeq:sealedSeq,seq:localSeq,pieces:unreported});sealedSeq=localSeq;unreported=[];flush()}
  }
 }
 function flush(){if(pending||!queue.length||!room||room.status==='finished'||ws.readyState!==1)return;const payload=queue[0],id='update-'+payload.seq;pending={payload,id,at:Date.now()};ws.send(JSON.stringify({...payload,requestId:id}))}
 function stop(){done=true;clearTimeout(deadline);clearInterval(timer);clearInterval(beat);ws.close(1000)}
 setTimeout(connect,index%20*25);
}
export function handleSummary(data){return {[__ENV.LOAD_SUMMARY||'runtime/summary.json']:JSON.stringify(data,null,2),stdout:JSON.stringify({event:'load_summary',metrics:data.metrics})}}
