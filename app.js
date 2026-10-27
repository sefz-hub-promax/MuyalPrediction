(function(){
var LS='family-election-guess';
var THRESH=4; // 3.25% ≈ 4 seats
var DEBOUNCE=700;
var S={guessers:[],parties:[],results:{},gmap:{}};
var raw={guessers:{},parties:{},results:{}}; // server rows by id; results = vals of row 'main'
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
function g(pid,gid){var gu=S.gmap[gid];var v=gu&&gu.values?gu.values[pid]:null;return isNum(v)?v:null;}
function newId(p){return p+Date.now().toString(36)+Math.random().toString(36).slice(2,5);}
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
function errText(e){return e&&e.transient?'אין חיבור לשרת — השינויים יישמרו כשהחיבור יחזור.':'השמירה נכשלה'+(e&&e.code?' ('+e.code+')':'')+'. הנתונים רועננו מהשרת.';}
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
function removeRow(table,id){
  dropQueued(id);gone[id]=true;delete raw[table][id];rebuild();render();
  after(id,function(){return oneOff(function(){return sb.from(table).delete().eq('id',id)})})
    .catch(function(){delete gone[id];loadAll();});
}

/* ---------- build state: server rows + local pending edits on top ---------- */
function rebuild(){
  var st=JSON.parse(JSON.stringify(raw));
  for(var k in Q){var q=Q[k];if(q.has)q.op.apply(st,q.val);else if(q.busy)q.op.apply(st,q.sent);}
  var gs=[],ps=[];
  for(var id in st.guessers){if(gone[id])continue;var d=st.guessers[id];gs.push({id:id,name:d.name||'',order:+d.ord||0,values:d.vals||{}});}
  for(var id2 in st.parties){if(gone[id2])continue;var d2=st.parties[id2];ps.push({id:id2,name:d2.name||'',order:+d2.ord||0,prev:isNum(d2.prev_2022)?d2.prev_2022:null,prevNote:d2.prev_note||'',leader:d2.leader||''});}
  var cmp=function(a,b){return a.order-b.order||(a.id<b.id?-1:1)};
  gs.sort(cmp);ps.sort(cmp);
  S={guessers:gs,parties:ps,results:st.results||{},gmap:{}};
  gs.forEach(function(x){S.gmap[x.id]=x});
}
function fixFocus(){if(!S.guessers.some(function(q){return q.id===focusG}))focusG=S.guessers[0]?S.guessers[0].id:null;}
function sig(){fixFocus();return [mode,view,focusG,S.parties.map(function(p){return p.id+':'+p.prevNote+':'+p.leader}).join(','),S.guessers.map(function(x){return x.id}).join(',')].join('|');}

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
    setVal('pn-'+p.id,p.name);setVal('pv-'+p.id,p.prev);
    S.guessers.forEach(function(gu){setVal('c-'+p.id+'-'+gu.id,g(p.id,gu.id));});
    setVal('r-'+p.id,isNum(S.results[p.id])?S.results[p.id]:null);
  });
  refresh();setStatus();
}

/* ---------- UI ---------- */
function remember(){try{localStorage.setItem(LS+'-view',JSON.stringify({view:view,g:focusG}));}catch(e){}}
function setView(v){view=v;remember();render();}
function hideCls(gid){return (view==='one'&&gid!==focusG)?'one-hide':'';}
function armDelete(btn,fn){
  if(btn.classList.contains('arm')){fn();return;}
  btn.classList.add('arm');btn.textContent='למחוק?';
  setTimeout(function(){btn.classList.remove('arm');btn.textContent='✕';},3000);
}

function render(){
  fixFocus();lastSig=sig();
  root.setAttribute('data-view',view);
  root.innerHTML='';
  root.appendChild(el('header',null,[
    el('div',null,[
      el('h1',null,[document.createTextNode('ניחוש בחירות '),el('span',{text:'משפחתי'})]),
      el('p',{class:'sub',text:'כל עמודה היא מנחש, כל שורה מפלגה. לחצו על שם או מספר כדי לערוך. כל עמודה נשמרת בנפרד, כך שכמה אנשים יכולים למלא בו־זמנית.'})
    ]),
    el('div',{class:'savebar'},[el('span',{id:'status',role:'status','aria-live':'polite',onclick:function(){if(failedCount())retryFailed();}})])
  ]));

  if(mode!=='live'){
    var msg={wait:'טוען את הניחושים…',noconfig:'חסרים פרטי החיבור ל־Supabase בקובץ config.js (ראו README).',loaderr:'לא הצלחנו לטעון את הנתונים. בדקו את החיבור לאינטרנט ונסו שוב.'}[mode];
    root.appendChild(el('section',{class:'sheet empty'},[el('p',{text:msg}),mode==='loaderr'?el('button',{class:'btn',text:'נסו שוב',onclick:function(){mode='wait';render();loadAll();}}):null]));
    setStatus();return;
  }

  var thr=el('tr',null,[el('th',{class:'party'},[el('span',{class:'lbl',text:'מפלגה'})]),el('th',{class:'prev'},[el('span',{class:'lbl',text:'בחירות 2022'})])]);
  S.guessers.forEach(function(gu){
    var inp=el('input',{class:'name',id:'gn-'+gu.id,value:gu.name,'aria-label':'שם מנחש',oninput:function(){edit('gf|'+gu.id,function(){return OP.field('guessers',gu.id,'name')},inp.value);var ch=document.getElementById('chip-'+gu.id);if(ch)ch.textContent=inp.value||'ללא שם';refresh();}});
    var x=el('button',{class:'x',text:'✕',title:'מחיקת מנחש',onclick:function(){armDelete(x,function(){removeRow('guessers',gu.id);});}});
    thr.appendChild(el('th',{class:hideCls(gu.id)},[x,inp]));
  });
  thr.appendChild(el('th',null,[el('span',{class:'lbl',text:'ממוצע'})]));
  thr.appendChild(el('th',{class:'res'},[el('span',{class:'lbl',text:'תוצאה בפועל'})]));

  var tb=el('tbody');
  S.parties.forEach(function(p){
    var pin=el('input',{id:'pn-'+p.id,value:p.name,'aria-label':'שם מפלגה',oninput:function(){edit('pf|'+p.id+'|name',function(){return OP.field('parties',p.id,'name')},pin.value);refresh();}});
    var px=el('button',{class:'x',text:'✕',title:'מחיקת מפלגה',onclick:function(){armDelete(px,function(){removeRow('parties',p.id);});}});
    var tr=el('tr',null,[el('td',{class:'party'},[el('div',{class:'cellwrap'},[el('div',{class:'pstack'},[pin,p.leader?el('small',{class:'leader',text:p.leader}):null]),px])])]);
    var pvi=el('input',{class:'num',type:'number',min:'0',max:'120',inputmode:'numeric',id:'pv-'+p.id,'aria-label':'מנדטים ב־2022 '+p.name,placeholder:'–',value:p.prev==null?'':p.prev,oninput:function(){edit('pf|'+p.id+'|prev',function(){return OP.field('parties',p.id,'prev_2022')},num(pvi.value));refresh();}});
    tr.appendChild(el('td',{class:'prev'},[pvi,p.prevNote?el('small',{text:p.prevNote}):null]));
    S.guessers.forEach(function(gu){
      var v=g(p.id,gu.id);
      var i=el('input',{class:'num',type:'number',min:'0',max:'120',inputmode:'numeric',id:'c-'+p.id+'-'+gu.id,'aria-label':gu.name+' — '+p.name,value:v==null?'':v,oninput:function(){
        edit('g|'+gu.id+'|'+p.id,function(){return OP.guess(gu.id,p.id)},num(i.value));refresh();}});
      tr.appendChild(el('td',{'data-p':p.id,'data-g':gu.id,class:hideCls(gu.id)},[i]));
    });
    tr.appendChild(el('td',{class:'avg',id:'avg-'+p.id}));
    var r=S.results[p.id];
    var ri=el('input',{class:'num',type:'number',min:'0',max:'120',inputmode:'numeric',id:'r-'+p.id,'aria-label':'תוצאה '+p.name,value:isNum(r)?r:'',oninput:function(){edit('r|'+p.id,function(){return OP.result(p.id)},num(ri.value));refresh();}});
    tr.appendChild(el('td',{class:'res','data-r':p.id},[ri]));
    tb.appendChild(tr);
  });

  var tf=el('tr',null,[el('td',{class:'party'},[el('span',{class:'lbl',text:'סה״כ מנדטים'})]),el('td',{class:'prev',id:'tot-prev'})]);
  S.guessers.forEach(function(gu){tf.appendChild(el('td',{id:'tot-'+gu.id,class:hideCls(gu.id)}));});
  tf.appendChild(el('td'));tf.appendChild(el('td',{class:'res',id:'tot-res'}));

  var table=el('table',null,[el('thead',null,[thr]),tb,el('tfoot',null,[tf])]);
  var adds=el('div',{class:'addrow'},[
    el('button',{class:'ghost',text:'+ מפלגה',onclick:function(){var id=newId('p');addRow('parties',{id:id,name:'מפלגה חדשה',ord:Date.now(),prev_2022:null,prev_note:''});render();var f=document.getElementById('pn-'+id);if(f){f.focus();f.select();}}}),
    el('button',{class:'ghost',text:'+ מנחש',onclick:function(){var id=newId('g');addRow('guessers',{id:id,name:'מנחש חדש',ord:Date.now(),vals:{}});focusG=id;remember();render();var f=document.getElementById('gn-'+id);if(f){f.focus();f.select();}}}),
    el('span',{class:'note',text:'מחיקה: לוחצים ✕ פעמיים. מספר כתום = מתחת לאחוז החסימה.'})
  ]);
  var vb=el('div',{class:'viewbar'},[
    el('div',{class:'seg',role:'group','aria-label':'תצוגה'},[
      el('button',{'aria-pressed':String(view==='one'),text:'לפי מנחש',onclick:function(){setView('one')}}),
      el('button',{'aria-pressed':String(view==='all'),text:'טבלה מלאה',onclick:function(){setView('all')}})
    ]),
    view==='one'?el('div',{class:'chips'},S.guessers.map(function(gu){return el('button',{id:'chip-'+gu.id,'aria-pressed':String(gu.id===focusG),text:gu.name||'ללא שם',onclick:function(){focusG=gu.id;remember();render();}})})):null
  ]);
  var sheet=el('section',{class:'sheet'},[vb,el('div',{class:'scroll'},[table]),adds]);
  root.appendChild(el('div',{class:'main'},[sheet,el('aside',{class:'sheet',id:'board'})]));
  refresh();setStatus();
}

function warnCell(td,v){
  if(!td)return;var w=below(v);td.classList.toggle('warn',w);
  var inp=td.querySelector('input');if(inp){if(w)inp.title='מתחת לאחוז החסימה: מפלגה שעוברת מקבלת לפחות '+THRESH+' מנדטים';else inp.removeAttribute('title');}
}
function scores(){
  var pids=S.parties.map(function(p){return p.id}).filter(function(id){return isNum(S.results[id])});
  return {n:pids.length,list:S.guessers.map(function(gu){
    var err=0,exact=0;pids.forEach(function(pid){var v=g(pid,gu.id)||0,r=S.results[pid];err+=Math.abs(v-r);if(g(pid,gu.id)===r)exact++;});
    return {gu:gu,err:err,exact:exact};
  }).sort(function(a,b){return a.err-b.err||b.exact-a.exact})};
}
function refresh(){
  var warns=0;
  S.parties.forEach(function(p){
    var vals=S.guessers.map(function(gu){return g(p.id,gu.id)}).filter(isNum);
    var a=document.getElementById('avg-'+p.id);if(a)a.textContent=vals.length?(vals.reduce(function(s,v){return s+v},0)/vals.length).toFixed(1):'–';
    S.guessers.forEach(function(gu){
      var td=root.querySelector('td[data-p="'+p.id+'"][data-g="'+gu.id+'"]');var v=g(p.id,gu.id);
      if(td){td.classList.toggle('hit',isNum(S.results[p.id])&&v===S.results[p.id]);warnCell(td,v);}
      if(below(v))warns++;
    });
    warnCell(root.querySelector('td[data-r="'+p.id+'"]'),S.results[p.id]);
  });
  S.guessers.forEach(function(gu){
    var t=0;S.parties.forEach(function(p){t+=g(p.id,gu.id)||0});
    var c=document.getElementById('tot-'+gu.id);if(c){c.textContent=t+'/120';c.className=(t>120?'over ':(t===120?'exact ':''))+hideCls(gu.id);}
  });
  var pt=0;S.parties.forEach(function(p){pt+=p.prev||0});var tp=document.getElementById('tot-prev');if(tp)tp.textContent=pt?pt+'/120':'';
  var rt=0;S.parties.forEach(function(p){rt+=isNum(S.results[p.id])?S.results[p.id]:0});
  var tr=document.getElementById('tot-res');if(tr)tr.textContent=rt?rt+'/120':'';
  renderBoard(warns);
}
function renderBoard(warns){
  var b=document.getElementById('board');if(!b)return;b.innerHTML='';
  var sc=scores();
  b.appendChild(el('h2',{text:'טבלת הדירוג'}));
  if(!sc.n){
    b.appendChild(el('p',{text:'עוד אין תוצאות. כשהספירה מתחילה, הזינו מנדטים בעמודה הכחולה. הניקוד הוא סך הפער מהתוצאה בכל המפלגות, כך שמי שקרוב ל־0 מוביל.'}));
    var ol=el('ol');
    S.parties.map(function(p){var vals=S.guessers.map(function(gu){return g(p.id,gu.id)}).filter(isNum);return {p:p,a:vals.length?vals.reduce(function(s,v){return s+v},0)/vals.length:0}}).sort(function(x,y){return y.a-x.a}).slice(0,5).forEach(function(o,i){
      ol.appendChild(el('li',{class:'rank'},[el('span',{class:'pos',text:String(i+1)}),el('span',{class:'nm'},[document.createTextNode(o.p.name),el('small',{text:'תחזית המשפחה בממוצע'})]),el('span',{class:'sc',text:o.a.toFixed(1)})]));
    });
    b.appendChild(ol);
  }else{
    var ol2=el('ol');
    sc.list.forEach(function(s,i){
      ol2.appendChild(el('li',{class:'rank'},[el('span',{class:'pos',text:String(i+1)}),el('span',{class:'nm'},[document.createTextNode(s.gu.name||'ללא שם'),el('small',{text:s.exact?('פגיעות מדויקות: '+s.exact):'אין פגיעה מדויקת'})]),el('span',{class:'sc',text:'פער '+s.err})]));
    });
    b.appendChild(ol2);
    b.appendChild(el('p',{class:'note',text:'מחושב לפי '+sc.n+' מפלגות עם תוצאה. ניחוש חסר נספר כ־0.'}));
  }
  if(warns)b.appendChild(el('p',{class:'warnnote',text:warns===1?'ניחוש אחד מתחת לאחוז החסימה (1–3 מנדטים). מפלגה שעוברת את 3.25% מקבלת לפחות 4.':warns+' ניחושים מתחת לאחוז החסימה (1–3 מנדטים). מפלגה שעוברת את 3.25% מקבלת לפחות 4.'}));
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
  st.textContent='✓ הכול שמור';
}

/* ---------- Supabase ---------- */
var loading=null;
function loadAll(){
  if(loading)return loading;
  loading=Promise.all([
    call(function(){return sb.from('parties').select('*')}),
    call(function(){return sb.from('guessers').select('*')}),
    call(function(){return sb.from('results').select('*').eq('id','main').maybeSingle()})
  ]).then(function(r){
    raw.parties=byId(r[0].data);raw.guessers=byId(r[1].data);
    raw.results=(r[2].data&&r[2].data.vals)||{};
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
    if(table==='results')raw.results={};else delete raw[table][oid];
  }else{
    var row=pl.new;if(!row||gone[row.id])return;
    if(table==='results'){if(row.id==='main')raw.results=row.vals||{};}
    else raw[table][row.id]=Object.assign({},raw[table][row.id],row);
  }
  if(mode==='live')liveRender();
};}

function start(){
  var cfg=window.APP_CONFIG||{};
  if(!window.supabase||!cfg.SUPABASE_URL||!cfg.SUPABASE_ANON_KEY||/YOUR-/.test(cfg.SUPABASE_URL+cfg.SUPABASE_ANON_KEY)){mode='noconfig';render();return;}
  sb=window.supabase.createClient(cfg.SUPABASE_URL,cfg.SUPABASE_ANON_KEY,{auth:{persistSession:false,autoRefreshToken:false}});
  render();loadAll();
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
start();
})();
