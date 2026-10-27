(function(){
var LS='family-election-guess';
var THRESH=4; // 3.25% ≈ 4 seats
var DEBOUNCE=700;
var S={guessers:[],parties:[],results:{},gmap:{}};
var raw={guessers:{},parties:{},results:{},polls:{}}; // server rows by id; results = vals of row 'main'; polls = exit polls, results rows 'poll*' (vals._name/_ord label them)
var pollSel=null;try{pollSel=localStorage.getItem(LS+'-poll');}catch(e){}
var gone={};   // ids deleted locally — ignore late realtime events for them
var ready={};  // id → promise<boolean> of a pending insert, so edits wait for the row to exist
var mode='wait'; // wait | live | noconfig | loaderr
var conn='ok', lastErr=null, busyN=0, lastSig=null;
var sb=null;
var root=document.getElementById('root');
var view='one',focusG=null;
try{var vv=JSON.parse(localStorage.getItem(LS+'-view')||'null');if(vv){view=vv.view==='all'?'all':'one';focusG=vv.g;}}catch(e){}

function el(tag,attrs,kids){var e=document.createElement(tag);if(attrs)for(var k in attrs){if(attrs[k]==null)continue;if(k==='text')e.textContent=attrs[k];else if(k.slice(0,2)==='on')e.addEventListener(k.slice(2),attrs[k]);else e.setAttribute(k,attrs[k]);}(kids||[]).forEach(function(c){if(c)e.appendChild(c)});return e;}
function num(v){return (v===''||v==null||isNaN(v))?null:Math.max(0,Math.min(120,Math.round(+v)));}
function isNum(v){return typeof v==='number'&&isFinite(v);}
function g(pid,gid){if(out(pid))return null;var gu=S.gmap[gid];var v=gu&&gu.values?gu.values[pid]:null;return isNum(v)?v:null;}
function newId(p){return p+Date.now().toString(36)+Math.random().toString(36).slice(2,5);}
function out(pid){var p=S.pmap&&S.pmap[pid];return !!p&&!p.running;}
function res(pid){return out(pid)?null:S.results[pid];}
function below(v){return isNum(v)&&v>0&&v<THRESH;}
function wait(ms){return new Promise(function(r){setTimeout(r,ms)});}
function byId(rows){var o={};(rows||[]).forEach(function(r){if(!gone[r.id])o[r.id]=r});return o;}

/* ---------- network: one call, retried once on transient errors ---------- */
function isTransient(e){return !e||!e.status||e.status>=500||e.status===408||e.status===429||/fetch|network|timeout|load failed/i.test(e.message||'');}
function call(fn){
  function once(){return Promise.resolve().then(fn).then(function(res){if(res&&res.error){var e=res.error;e.status=res.status;throw e;}return res;});}
  return once().catch(function(e){if(!isTransient(e))throw e;return wait(1200).then(once);})
    .catch(function(e){e=e||{};e.transient=isTransient(e);throw e;});
}
function errText(e){if(e&&(e.hint==='locked'||/locked/.test(e.message||'')||e.code==='42501'))return 'הניחושים כבר נעולים — השינוי לא נשמר.';return e&&e.transient?'אין חיבור לשרת — השינויים יישמרו כשהחיבור יחזור.':'השמירה נכשלה'+(e&&e.code?' ('+e.code+')':'')+'. הנתונים רועננו מהשרת.';}
function oneOff(fn){
  busyN++;setStatus();
  return call(fn).then(function(r){busyN--;lastErr=null;setStatus();return r;},function(e){busyN--;lastErr=errText(e);setStatus();throw e;});
}
function after(id,fn){return (ready[id]||Promise.resolve(true)).then(function(ok){return ok?fn():{error:{message:'row was not created',status:400}};});}

/* ---------- per-cell write queue: each cell / field saves on its own ---------- */
var Q={}; // key → {op, val, has, sent, busy, timer, failed}
var OP={
  guess:function(gid,pid){return {id:gid,send:function(v){return after(gid,function(){return sb.rpc('set_guess',{g:gid,p:pid,v:v})})},
    apply:function(st,v){var r=st.guessers[gid];if(!r)return;r.vals=r.vals||{};if(v==null)delete r.vals[pid];else r.vals[pid]=v;}};},
  result:function(pid){return {id:'results',send:function(v){return sb.rpc('set_result',{p:pid,v:v})},
    apply:function(st,v){st.results=st.results||{};if(v==null)delete st.results[pid];else st.results[pid]=v;}};},
  field:function(table,id,col){return {id:id,send:function(v){var o={};o[col]=v;return after(id,function(){return sb.from(table).update(o).eq('id',id)})},
    apply:function(st,v){var r=st[table][id];if(r)r[col]=v;}};}
};
function edit(key,mk,v){
  var q=Q[key]||(Q[key]={op:mk()});
  q.val=v;q.has=true;q.failed=false;
  clearTimeout(q.timer);q.timer=setTimeout(function(){q.timer=null;flush(key)},DEBOUNCE);
  rebuild();setStatus();
}
function flush(key){
  var q=Q[key];if(!q||!q.has||q.busy)return;
  var v=q.val;q.has=false;q.busy=true;q.sent=v;setStatus();
  call(function(){return q.op.send(v)}).then(function(){
    q.busy=false;q.op.apply(raw,v);lastErr=null;settle(key,q);
  },function(e){
    q.busy=false;lastErr=errText(e);
    if(e.transient){if(!q.has){q.has=true;q.val=v;}q.failed=true;}
    else loadAll(); // e.g. row deleted by someone else — drop this value and resync
    settle(key,q);
  });
}
function settle(key,q){
  if(Q[key]!==q){setStatus();return;} // row was deleted meanwhile
  if(q.has&&!q.timer&&!q.failed)flush(key);
  else if(!q.has&&!q.busy)delete Q[key];
  rebuild();liveRender();setStatus();
}
function retryFailed(){for(var k in Q){if(Q[k].failed){Q[k].failed=false;flush(k);}}}
function anyPending(){if(busyN)return true;for(var k in Q){if(Q[k].has||Q[k].busy)return true;}return false;}
function failedCount(){var n=0;for(var k in Q)if(Q[k].failed)n++;return n;}
function dropQueued(id){for(var k in Q){if(Q[k].op.id===id){clearTimeout(Q[k].timer);delete Q[k];}}}

/* ---------- inserts / deletes ---------- */
function addRow(table,row){
  raw[table][row.id]=row;
  ready[row.id]=oneOff(function(){return sb.from(table).insert(row)}).then(function(){return true},function(){
    delete raw[table][row.id];rebuild();render();return false;});
  rebuild();
}

/* ---------- build state: server rows + local pending edits on top ---------- */
function rebuild(){
  var st=JSON.parse(JSON.stringify(raw));
  for(var k in Q){var q=Q[k];if(q.has)q.op.apply(st,q.val);else if(q.busy)q.op.apply(st,q.sent);}
  var gs=[],ps=[];
  for(var id in st.guessers){if(gone[id])continue;var d=st.guessers[id];gs.push({id:id,name:d.name||'',order:+d.ord||0,values:d.vals||{}});}
  for(var id2 in st.parties){if(gone[id2])continue;var d2=st.parties[id2];ps.push({id:id2,name:d2.name||'',order:+d2.ord||0,prev:isNum(d2.prev_2022)?d2.prev_2022:null,prevNote:d2.prev_note||'',leader:d2.leader||'',running:d2.running!==false});}
  var cmp=function(a,b){return a.order-b.order||(a.id<b.id?-1:1)};
  gs.sort(cmp);ps.sort(cmp);
  var pl=Object.keys(st.polls||{}).map(function(id){var v=st.polls[id]||{};return {id:id,name:v._name||'מדגם',order:+v._ord||0,vals:v};}).sort(function(a,b){return a.order-b.order||(a.id<b.id?-1:1)});
  var cur=pl.filter(function(x){return x.id===pollSel})[0]||pl[0];
  S={guessers:gs,parties:ps,results:st.results||{},polls:pl,pollId:cur?cur.id:null,pollName:cur?cur.name:'',poll:cur?cur.vals:{},gmap:{},pmap:{}};
  ps.forEach(function(x){S.pmap[x.id]=x});
  gs.forEach(function(x){S.gmap[x.id]=x});
}
function fixFocus(){if(!S.guessers.some(function(q){return q.id===focusG}))focusG=S.guessers[0]?S.guessers[0].id:null;}
function sig(){fixFocus();return [mode,view,focusG,S.parties.map(function(p){return p.id+':'+p.prevNote+':'+p.leader+':'+p.running}).join(','),S.guessers.map(function(x){return x.id}).join(','),resVisible(),lockAt,locked(),S.pollId,JSON.stringify(S.polls)].join('|');}

/* remote changes: patch values in place when the table shape is unchanged (keeps focus + mobile keyboard),
   otherwise re-render and restore focus/caret in the field being typed in */
var rafP=false;
function liveRender(){if(rafP)return;rafP=true;requestAnimationFrame(function(){rafP=false;rebuild();if(sig()===lastSig)patch();else fullRender();});}
function fullRender(){
  var a=document.activeElement,id=a&&a.id,val=a&&a.value,ss=null,se=null;
  try{ss=a.selectionStart;se=a.selectionEnd;}catch(e){}
  render();
  if(id){var n=document.getElementById(id);if(n&&n.tagName==='INPUT'){n.value=val;n.focus({preventScroll:true});try{if(ss!=null)n.setSelectionRange(ss,se);}catch(e){}}}
}
function setVal(id,v){var n=document.getElementById(id);if(!n||n===document.activeElement)return;var s=v==null?'':String(v);if(n.value!==s)n.value=s;}
function patch(){
  S.guessers.forEach(function(gu){
    setVal('gn-'+gu.id,gu.name);
    var ch=document.getElementById('chip-'+gu.id);if(ch)ch.textContent=gu.name||'ללא שם';
  });
  S.parties.forEach(function(p){
    setVal('pn-'+p.id,p.name);var tki=document.querySelector('#pt-'+p.id+' .tk-in');if(tki&&tki.textContent!==p.name)tki.textContent=p.name;setVal('pv-'+p.id,p.prev);
    S.guessers.forEach(function(gu){setVal('c-'+p.id+'-'+gu.id,g(p.id,gu.id));});
    setVal('r-'+p.id,isNum(res(p.id))?res(p.id):null);
  });
  refresh();setStatus();fitTickers();
}

/* ---------- UI ---------- */
function remember(){try{localStorage.setItem(LS+'-view',JSON.stringify({view:view,g:focusG}));}catch(e){}}
function setView(v){view=v;remember();render();}

var showRes=false,lockAt=null,lockTimer=null;
function locked(){return lockAt!=null&&Date.now()>=lockAt;}
function lockTime(){return new Date(lockAt).toLocaleTimeString('he-IL',{hour:'2-digit',minute:'2-digit',timeZone:'Asia/Jerusalem'});}
function lockLine(){if(lockAt==null)return null;return el('p',{class:'lockline'+(locked()?' on':'')},[document.createTextNode(locked()?'🔒 הניחושים נעולים מ־'+lockTime()+' · אפשר להזין רק תוצאות':'⏰ הניחושים ננעלים היום ב־'+lockTime())]);}
function loadLock(){
  call(function(){return sb.rpc('lock_at')}).then(function(r){
    if(!r.data)return;lockAt=new Date(r.data).getTime();clearTimeout(lockTimer);
    if(!locked())lockTimer=setTimeout(function(){if(mode==='live')fullRender();},lockAt-Date.now()+300);
    if(mode==='live')fullRender();
  },function(){});
}
function resVisible(){return showRes||S.parties.some(function(p){return isNum(res(p.id))});}
function fitTickers(){
  root.querySelectorAll('.ticker').forEach(function(t){
    var i=t.firstChild,d=isPhone()&&i?Math.ceil(i.offsetWidth-t.clientWidth):0;
    t.classList.toggle('run',d>2);t.style.setProperty('--dist',d+'px');t.style.setProperty('--dur',(3+d/22).toFixed(1)+'s');
  });
}
function hasPoll(){return S.parties.some(function(p){return isNum(S.poll[p.id])});}
function isPhone(){return window.matchMedia('(max-width:640px)').matches;}
function showG(gid){return !(isPhone()&&view==='one')||gid===focusG;}
function gridCols(){
  var one=isPhone()&&view==='one',n=S.guessers.filter(function(gu){return showG(gu.id)}).length;
  var rc=resVisible();
  var pc=hasPoll();
  if(one)return 'minmax(0,1fr) 40px 62px 40px'+(pc?' 42px':'')+(rc?' 50px':'');
  return (isPhone()?'100px 52px':'170px 76px')+' repeat('+n+',64px) 62px'+(pc?(isPhone()?' 50px':' 62px'):'')+(rc?' 76px':'');
}

function render(){
  fixFocus();lastSig=sig();
  var L=locked()?'':null;
  root.setAttribute('data-view',view);
  root.innerHTML='';
  var ring=el('div',{class:'ring','aria-hidden':'true'});
  var head=el('header',null,[
    el('div',{class:'titles'},[
      el('h1',{text:'ניחוש בחירות'}),
      el('p',{class:'sub',text:'כנסת ה־26 · כל אחד ממלא את הטור שלו'}),lockLine(),
      S.polls.length>1?el('div',{class:'pollbar'},[el('span',{class:'pollbar-lbl',text:'מדגם:'})].concat(S.polls.map(function(x){return el('button',{class:'pchip','aria-pressed':String(x.id===S.pollId),text:x.name,onclick:function(){pollSel=x.id;try{localStorage.setItem(LS+'-poll',x.id);}catch(e){}rebuild();render();}})}))):null
    ]),
    el('span',{id:'status',role:'status','aria-live':'polite',onclick:function(){if(failedCount())retryFailed();}})
  ]);
  var paper=el('div',{class:'paper'},[ring,head]);
  var napkin=el('section',{class:'napkin'},[paper]);
  var receipt=el('aside',{class:'receipt'},[el('div',{class:'tear top','aria-hidden':'true'}),el('div',{class:'slip',id:'board'}),el('div',{class:'tear bottom','aria-hidden':'true'})]);
  root.appendChild(el('div',{class:'stage'},[napkin,receipt]));

  if(mode!=='live'){
    var msg={wait:'טוען את הניחושים…',noconfig:'חסרים פרטי החיבור ל־Supabase בקובץ config.js (ראו README).',loaderr:'לא הצלחנו לטעון את הנתונים. בדקו את החיבור לאינטרנט ונסו שוב.'}[mode];
    paper.appendChild(el('div',{class:'empty'},[el('p',{text:msg}),mode==='loaderr'?el('button',{class:'pen-btn',text:'נסו שוב',onclick:function(){mode='wait';render();loadAll();}}):null]));
    receipt.style.display='none';setStatus();return;
  }

  // phone: pick whose column to show
  var fg=S.gmap[focusG];
  paper.appendChild(el('div',{class:'viewbar'},[
    view==='one'?el('div',{class:'chips'},S.guessers.map(function(gu){return el('button',{class:'chip',id:'chip-'+gu.id,'aria-pressed':String(gu.id===focusG),text:gu.name||'ללא שם',onclick:function(){focusG=gu.id;remember();render();}})})):null,
    el('div',{class:'viewline'},view==='one'?[
      el('span',{class:'mine',text:'הטור של '+(fg&&fg.name||'')}),
      el('button',{class:'link',text:'טבלה מלאה ←',onclick:function(){setView('all')}})
    ]:[
      el('span',{class:'mine',text:'כל הטורים'}),
      el('button',{class:'link',text:'→ לפי מנחש',onclick:function(){setView('one')}})
    ])
  ]));

  var tbl=el('div',{class:'tbl'+(isPhone()&&view==='one'?' fit':''),style:'--cols:'+gridCols()});
  var hr=el('div',{class:'grid head'},[el('div',{class:'cell party'},[el('span',{class:'lbl',text:'מפלגה'})]),el('div',{class:'cell prev'},[el('span',{class:'lbl pencil-lbl',text:'2022'})])]);
  S.guessers.forEach(function(gu){
    if(!showG(gu.id))return;
    var inp=el('input',{class:'gname',id:'gn-'+gu.id,value:gu.name,'aria-label':'שם מנחש',disabled:L,oninput:function(){edit('gf|'+gu.id,function(){return OP.field('guessers',gu.id,'name')},inp.value);var ch=document.getElementById('chip-'+gu.id);if(ch)ch.textContent=inp.value||'ללא שם';refresh();}});
    hr.appendChild(el('div',{class:'cell gh'},[inp]));
  });
  hr.appendChild(el('div',{class:'cell'},[el('span',{class:'lbl',text:'ממוצע'})]));
  if(hasPoll())hr.appendChild(el('div',{class:'cell pollh'},[el('span',{class:'lbl poll-lbl',text:'מדגם'}),el('small',{class:'pollname',text:S.pollName})]));
  if(resVisible())hr.appendChild(el('div',{class:'cell res'},[el('span',{class:'lbl red-lbl',text:'בפועל'})]));
  tbl.appendChild(hr);

  S.parties.forEach(function(p,ri){
    var dis=p.running?null:'';
    var tk=el('span',{class:'tk-in',text:p.name});
    var pin=el('input',{class:'pname',id:'pn-'+p.id,value:p.name,'aria-label':'שם מפלגה',disabled:L,onblur:fitTickers,oninput:function(){tk.textContent=pin.value;edit('pf|'+p.id+'|name',function(){return OP.field('parties',p.id,'name')},pin.value);refresh();}});
    var row=el('div',{class:'grid row'+(ri%2?' alt':'')+(p.running?'':' out')},[
      el('div',{class:'cell party'},[el('div',{class:'pstack'},[el('div',{class:'pname-box'},[el('span',{class:'ticker',id:'pt-'+p.id,'aria-hidden':'true'},[tk]),pin]),p.leader?el('small',{class:'leader ticker'},[el('span',{class:'tk-in',text:p.leader})]):null,p.running?null:el('small',{class:'notrun',text:'לא מתמודדת'})])])
    ]);
    var pvi=el('input',{class:'num',type:'number',min:'0',max:'120',inputmode:'numeric',id:'pv-'+p.id,'aria-label':'מנדטים ב־2022 '+p.name,placeholder:'–',value:p.prev==null?'':p.prev,disabled:L,oninput:function(){edit('pf|'+p.id+'|prev',function(){return OP.field('parties',p.id,'prev_2022')},num(pvi.value));refresh();}});
    row.appendChild(el('div',{class:'cell prev'},[pvi,p.prevNote?el('small',{class:'pnote ticker'},[el('span',{class:'tk-in',text:p.prevNote})]):null]));
    S.guessers.forEach(function(gu){
      if(!showG(gu.id))return;
      var v=g(p.id,gu.id);
      var i=el('input',{class:'num',type:'number',min:'0',max:'120',inputmode:'numeric',id:'c-'+p.id+'-'+gu.id,'aria-label':gu.name+' — '+p.name,placeholder:'–',value:v==null?'':v,disabled:(dis!=null||L!=null)?'':null,oninput:function(){
        edit('g|'+gu.id+'|'+p.id,function(){return OP.guess(gu.id,p.id)},num(i.value));refresh();}});
      row.appendChild(el('div',{class:'cell gc','data-p':p.id,'data-g':gu.id},[el('span',{class:'mark'},[i])]));
    });
    row.appendChild(el('div',{class:'cell avg',id:'avg-'+p.id}));
    if(hasPoll())row.appendChild(el('div',{class:'cell poll'},[el('span',{text:isNum(S.poll[p.id])?String(S.poll[p.id]):'–'})]));
    var r=res(p.id);
    var rin=el('input',{class:'num',type:'number',min:'0',max:'120',inputmode:'numeric',id:'r-'+p.id,'aria-label':'תוצאה '+p.name,placeholder:p.running?' ':'–',value:isNum(r)?r:'',disabled:dis,oninput:function(){edit('r|'+p.id,function(){return OP.result(p.id)},num(rin.value));refresh();}});
    if(resVisible())row.appendChild(el('div',{class:'cell res','data-r':p.id},[el('span',{class:'mark'},[rin])]));
    tbl.appendChild(row);
  });

  var sr=el('div',{class:'grid sum'},[el('div',{class:'cell party'},[el('span',{class:'lbl',text:'סה״כ'})]),el('div',{class:'cell prev',id:'tot-prev'})]);
  S.guessers.forEach(function(gu){if(showG(gu.id))sr.appendChild(el('div',{class:'cell tot',id:'tot-'+gu.id}));});
  sr.appendChild(el('div',{class:'cell'}));
  if(hasPoll())sr.appendChild(el('div',{class:'cell poll'},[el('span',{text:S.parties.reduce(function(t,p){return t+(isNum(S.poll[p.id])?S.poll[p.id]:0)},0)+'/120'})]));
  if(resVisible())sr.appendChild(el('div',{class:'cell res',id:'tot-res'}));
  tbl.appendChild(sr);
  paper.appendChild(el('div',{class:'scroll'},[tbl]));

  paper.appendChild(el('div',{class:'addrow'},[
    L!=null?null:el('button',{class:'pen-btn',text:'+ מפלגה',onclick:function(){var id=newId('p');addRow('parties',{id:id,name:'מפלגה חדשה',ord:Date.now(),prev_2022:null,prev_note:''});render();var f=document.getElementById('pn-'+id);if(f){f.focus();f.select();}}}),
    L!=null?null:el('button',{class:'pen-btn',text:'+ מנחש',onclick:function(){var id=newId('g');addRow('guessers',{id:id,name:'מנחש חדש',ord:Date.now(),vals:{}});focusG=id;remember();render();var f=document.getElementById('gn-'+id);if(f){f.focus();f.select();}}}),
    resVisible()?null:el('button',{class:'pen-btn red-btn',text:'+ תוצאות אמת',onclick:function(){showRes=true;render();var f=root.querySelector('.cell.res input:not(:disabled)');if(f)f.focus();}}),
    el('span',{class:'legend',text:resVisible()?'עיפרון = 2022 · עט אדום = התוצאה בפועל':'עיפרון = 2022 · עמודת התוצאות תופיע כשיתחילו להזין תוצאות'})
  ]));
  refresh();setStatus();fitTickers();
}

function warnCell(cell,v){
  if(!cell)return;var w=below(v);cell.classList.toggle('warn',w);
  var inp=cell.querySelector('input');if(inp){if(w)inp.title='מתחת לאחוז החסימה: מפלגה שעוברת מקבלת לפחות '+THRESH+' מנדטים';else inp.removeAttribute('title');}
}
function scores(){
  var pids=S.parties.map(function(p){return p.id}).filter(function(id){return isNum(res(id))});
  return {n:pids.length,list:S.guessers.map(function(gu){
    var err=0,exact=0;pids.forEach(function(pid){var v=g(pid,gu.id)||0,r=res(pid);err+=Math.abs(v-r);if(g(pid,gu.id)===r)exact++;});
    return {gu:gu,err:err,exact:exact};
  }).sort(function(a,b){return a.err-b.err||b.exact-a.exact})};
}
function refresh(){
  var warns=0;
  S.parties.forEach(function(p){
    var vals=S.guessers.map(function(gu){return g(p.id,gu.id)}).filter(isNum);
    var a=document.getElementById('avg-'+p.id);if(a)a.textContent=vals.length?(vals.reduce(function(s,v){return s+v},0)/vals.length).toFixed(1):'–';
    S.guessers.forEach(function(gu){
      var c=root.querySelector('.cell[data-p="'+p.id+'"][data-g="'+gu.id+'"]');var v=g(p.id,gu.id);
      if(c){c.classList.toggle('hit',isNum(res(p.id))&&v===res(p.id));c.classList.toggle('phit',!out(p.id)&&isNum(S.poll[p.id])&&v===S.poll[p.id]);warnCell(c,v);}
      if(below(v))warns++;
    });
    var rc=root.querySelector('.cell[data-r="'+p.id+'"]');warnCell(rc,res(p.id));
    if(rc)rc.classList.toggle('filled',isNum(res(p.id)));
  });
  S.guessers.forEach(function(gu){
    var t=0;S.parties.forEach(function(p){t+=g(p.id,gu.id)||0});
    var c=document.getElementById('tot-'+gu.id);if(c){c.textContent=t+'/120';c.className='cell tot'+(t>120?' over':(t===120?' exact':''));}
  });
  var pt=0;S.parties.forEach(function(p){pt+=p.prev||0});var tp=document.getElementById('tot-prev');if(tp)tp.textContent=pt?String(pt):'';
  var rt=0;S.parties.forEach(function(p){rt+=isNum(res(p.id))?res(p.id):0});
  var tr=document.getElementById('tot-res');if(tr)tr.textContent=rt?rt+'/120':'';
  renderBoard(warns);
}
function renderBoard(warns){
  var b=document.getElementById('board');if(!b)return;b.innerHTML='';
  var sc=scores();
  b.appendChild(el('div',{class:'rhead'},[el('span',{class:'stars',text:'*** חשבון ***'}),el('h2',{text:'טבלת הדירוג'}),
    el('span',{class:'rsub',text:sc.n?'מחושב לפי '+sc.n+' מפלגות עם תוצאה':'עוד אין תוצאות. בינתיים: תחזית המשפחה בממוצע'})]));
  b.appendChild(el('hr'));
  var ol=el('ol');
  if(!sc.n){
    S.parties.filter(function(p){return p.running}).map(function(p){var vals=S.guessers.map(function(gu){return g(p.id,gu.id)}).filter(isNum);return {p:p,a:vals.length?vals.reduce(function(s,v){return s+v},0)/vals.length:0}}).sort(function(x,y){return y.a-x.a}).slice(0,5).forEach(function(o,i){
      ol.appendChild(el('li',null,[el('span',{class:'pos',text:(i+1)+'.'}),el('span',{class:'nm',text:o.p.name}),el('span',{class:'dots'}),el('span',{class:'sc',text:o.a.toFixed(1)})]));
    });
  }else{
    sc.list.forEach(function(s,i){
      ol.appendChild(el('li',{class:i===0?'lead':null},[el('span',{class:'pos',text:(i+1)+'.'}),el('span',{class:'nm'},[document.createTextNode(s.gu.name||'ללא שם'),s.exact?el('small',{text:'פגיעות מדויקות: '+s.exact}):null]),el('span',{class:'dots'}),el('span',{class:'sc',text:'פער '+s.err})]));
    });
  }
  b.appendChild(ol);
  b.appendChild(el('hr'));
  b.appendChild(el('p',{class:'rnote',text:'ניקוד: סך הפער מהתוצאה בכל המפלגות. הכי קרוב ל־0 מנצח. ניחוש חסר נספר כ־0.'}));
  if(warns)b.appendChild(el('p',{class:'rnote warnnote',text:(warns===1?'ניחוש אחד':warns+' ניחושים')+' מתחת לאחוז החסימה (1–3 מנדטים). מפלגה שעוברת את 3.25% מקבלת לפחות 4.'}));
  b.appendChild(el('p',{class:'thanks',text:S.guessers.length+' מנחשים · תודה ובהצלחה'}));
}
function setStatus(){
  var st=document.getElementById('status');if(!st)return;
  st.className='';st.removeAttribute('title');
  if(mode!=='live'){st.textContent='';return;}
  var f=failedCount();
  if(f){st.textContent='⚠ '+f+' שינויים לא נשמרו — לחצו לנסות שוב';st.className='err retry';st.title=lastErr||'';return;}
  if(lastErr){st.textContent=lastErr;st.className='err';return;}
  if(anyPending()){st.textContent='שומר…';st.className='dirty';return;}
  if(conn==='down'){st.textContent='✓ הכול שמור · מתחבר מחדש לעדכונים חיים…';st.className='dirty';return;}
  st.textContent='✓ נשמר';st.className='ok';
}

/* ---------- Supabase ---------- */
var loading=null;
function loadAll(){
  if(loading)return loading;
  loading=Promise.all([
    call(function(){return sb.from('parties').select('*')}),
    call(function(){return sb.from('guessers').select('*')}),
    call(function(){return sb.from('results').select('*')})
  ]).then(function(r){
    raw.parties=byId(r[0].data);raw.guessers=byId(r[1].data);
    var rr=byId(r[2].data);raw.results=(rr.main&&rr.main.vals)||{};raw.polls={};for(var pk in rr)if(/^poll/.test(pk))raw.polls[pk]=rr[pk].vals||{};
    var first=mode!=='live';mode='live';
    if(first){rebuild();render();}else liveRender();
  },function(e){
    console.error('load failed',e);
    if(mode!=='live'){mode='loaderr';render();}
    else{lastErr=errText(e);setStatus();}
  }).then(function(){loading=null;});
  return loading;
}
function onChange(table){return function(pl){
  if(pl.eventType==='DELETE'){
    var oid=pl.old&&pl.old.id;if(!oid)return;
    if(table==='results'){if(/^poll/.test(oid))delete raw.polls[oid];else raw.results={};}else delete raw[table][oid];
  }else{
    var row=pl.new;if(!row||gone[row.id])return;
    if(table==='results'){if(row.id==='main')raw.results=row.vals||{};else if(/^poll/.test(row.id))raw.polls[row.id]=row.vals||{};}
    else raw[table][row.id]=Object.assign({},raw[table][row.id],row);
  }
  if(mode==='live')liveRender();
};}

function start(){
  var cfg=window.APP_CONFIG||{};
  if(!window.supabase||!cfg.SUPABASE_URL||!cfg.SUPABASE_ANON_KEY||/YOUR-/.test(cfg.SUPABASE_URL+cfg.SUPABASE_ANON_KEY)){mode='noconfig';render();return;}
  sb=window.supabase.createClient(cfg.SUPABASE_URL,cfg.SUPABASE_ANON_KEY,{auth:{persistSession:false,autoRefreshToken:false}});
  render();loadAll();loadLock();
  var ch=sb.channel('election');
  ['parties','guessers','results'].forEach(function(t){ch.on('postgres_changes',{event:'*',schema:'public',table:t},onChange(t));});
  ch.subscribe(function(status){
    // every (re)subscribe re-reads everything so events missed while disconnected are caught up
    if(status==='SUBSCRIBED'){conn='ok';setStatus();loadAll();}
    else if(status==='CHANNEL_ERROR'||status==='TIMED_OUT'||status==='CLOSED'){conn='down';setStatus();}
  });
  window.addEventListener('online',function(){retryFailed();loadAll();});
  document.addEventListener('visibilitychange',function(){if(document.visibilityState==='visible'&&mode==='live'){retryFailed();loadAll();}});
}
try{document.fonts&&document.fonts.ready.then(function(){if(mode==='live')fitTickers();});}catch(e){}
try{window.matchMedia('(max-width:640px)').addEventListener('change',function(){if(mode==='live')render();});}catch(e){}
start();
})();
