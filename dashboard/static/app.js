(function(){"use strict";
var ws=null,lastIn=0,lastOut=0,lastTs=Date.now();

async function init(){
    try{
        var r=await fetch("/api/pipeline");
        var p=await r.json();
        renderFlow(p);
        renderSource(p.source);
        renderOps(p.operators);
        renderSink(p.sink);
        if(p.checkpoint) renderCP(p.checkpoint);
    }catch(e){
        document.getElementById("flow").innerHTML='<p class="loading">Error: '+e.message+'</p>';
    }
    connectWS();
}

// ---- Flow graph ----
function renderFlow(p){
    var el=document.getElementById("flow");
    el.innerHTML="";
    var d=document.createElement("div");d.className="flow";
    d.appendChild(flowNode("s",p.source.type,sourceSummary(p.source)));
    p.operators.forEach(function(o,i){
        d.appendChild(arrowEl());
        d.appendChild(flowNode("o",o.label||o.type,o.type));
    });
    d.appendChild(arrowEl());
    d.appendChild(flowNode("k",p.sink.type,sinkSummary(p.sink)));
    el.appendChild(d);
}
function flowNode(css,title,meta){
    var n=document.createElement("div");n.className="node node-"+css;
    var t=document.createElement("span");t.className="node-type";
    t.textContent=css==="s"?"Source":css==="k"?"Sink":"Operator";n.appendChild(t);
    var h=document.createElement("span");h.className="node-title";h.textContent=title;n.appendChild(h);
    if(meta){var m=document.createElement("span");m.className="node-meta";m.textContent=meta;n.appendChild(m)}
    return n;
}
function arrowEl(){var a=document.createElement("span");a.className="arrow";a.innerHTML="&#8594;";return a}
function sourceSummary(s){return s.props&&s.props.topic?s.props.topic:s.props&&s.props.topics?s.props.topics:""}
function sinkSummary(s){return s.props&&s.props.topic?s.props.topic:s.props&&s.props.table?s.props.table:""}

// ---- Source detail ----
function renderSource(s){
    var rows=[["Type",s.type]];
    if(s.props)Object.keys(s.props).sort().forEach(function(k){rows.push([k,s.props[k]])});
    document.getElementById("source-card").querySelector(".card-body").innerHTML=tableHTML(rows);
}

// ---- Operators ----
function renderOps(ops){
    var html="";
    ops.forEach(function(o,i){
        html+='<div style="margin-bottom:0.4rem"><strong>'+(i+1)+". "+o.label+'</strong> <span style="color:var(--dim)">'+o.type+"</span></div>";
        if(o.config){
            html+='<table class="config-table" style="margin-bottom:0.75rem">';
            Object.keys(o.config).sort().forEach(function(k){html+="<tr><td>"+k+"</td><td>"+o.config[k]+"</td></tr>"});
            html+="</table>";
        }
    });
    if(!html)html='<p class="loading">No operators defined</p>';
    document.getElementById("ops-card").querySelector(".card-body").innerHTML=html;
}

// ---- Sink detail ----
function renderSink(s){
    var rows=[["Type",s.type]];
    if(s.props)Object.keys(s.props).sort().forEach(function(k){rows.push([k,s.props[k]])});
    document.getElementById("sink-card").querySelector(".card-body").innerHTML=tableHTML(rows);
}

// ---- Checkpoint ----
function renderCP(c){
    var rows=[["Interval",c.interval],["Storage",c.storage]];
    document.getElementById("cp-body").innerHTML=tableHTML(rows);
    document.getElementById("cp-section").classList.remove("hidden");
}

// ---- Helpers ----
function tableHTML(rows){
    var h='<table class="config-table">';
    rows.forEach(function(r){h+="<tr><td>"+r[0]+"</td><td>"+r[1]+"</td></tr>"});
    return h+"</table>";
}

// ---- WebSocket ----
function connectWS(){
    var proto=location.protocol==="https:"?"wss:":"ws:";
    ws=new WebSocket(proto+"//"+location.host+"/ws");
    ws.onmessage=function(e){
        var s=JSON.parse(e.data);
        updateStatus(s);
    };
    ws.onclose=function(){setTimeout(connectWS,2000)};
    ws.onerror=function(){ws.close()};
}
function updateStatus(s){
    var dot=document.getElementById("status-ind");
    var txt=document.getElementById("status-text");
    var upt=document.getElementById("uptime");
    if(s.running){dot.className="dot on";txt.textContent="Running"}
    else{dot.className="dot off";txt.textContent="Stopped"}
    upt.textContent=s.uptime?"uptime: "+s.uptime:"";
    document.getElementById("m-in").textContent=s.records_in||0;
    document.getElementById("m-out").textContent=s.records_out||0;

    var now=Date.now(),elapsed=(now-lastTs)/1000;
    if(elapsed>0.5){
        var rateIn=Math.round((s.records_in-lastIn)/elapsed);
        var rateOut=Math.round((s.records_out-lastOut)/elapsed);
        document.getElementById("m-rate").textContent=Math.max(rateIn,rateOut)+"/s";
        lastIn=s.records_in;lastOut=s.records_out;lastTs=now;
    }
    if(s.lag!==undefined){document.getElementById("m-lag").textContent=s.lag}else{document.getElementById("m-lag").textContent="-"}
    if(s.error){document.getElementById("err-section").classList.remove("hidden");document.getElementById("err-text").textContent=s.error}
}
init();
})();
