(function () {
    "use strict";

    let pipeline = null;
    let ws = null;

    // ---- Pipeline graph rendering ----

    async function loadPipeline() {
        try {
            const res = await fetch("/api/pipeline");
            pipeline = await res.json();
            renderPipeline(pipeline);
        } catch (err) {
            document.querySelector("#pipeline-graph").innerHTML =
                '<p class="loading">Failed to load pipeline: ' + err.message + "</p>";
        }
    }

    function renderPipeline(p) {
        const graph = document.querySelector("#pipeline-graph");
        graph.innerHTML = "";

        const flow = document.createElement("div");
        flow.className = "pipeline-flow";

        // Source node
        flow.appendChild(createNode("source", p.source.type, p.source.props));

        // Operator nodes with arrows between
        p.operators.forEach(function (op) {
            flow.appendChild(createArrow());
            const title = op.label || op.type;
            flow.appendChild(createNode("op", title, { type: op.type }));
        });

        // Sink node
        flow.appendChild(createArrow());
        flow.appendChild(createNode("sink", p.sink.type, p.sink.props));

        graph.appendChild(flow);
    }

    function createNode(kind, title, props) {
        const node = document.createElement("div");
        node.className = "node node-" + kind;

        const typeEl = document.createElement("span");
        typeEl.className = "node-type";
        typeEl.textContent = kind === "source" ? "Source" : kind === "sink" ? "Sink" : "Operator";
        node.appendChild(typeEl);

        const titleEl = document.createElement("span");
        titleEl.className = "node-title";
        titleEl.textContent = title;
        node.appendChild(titleEl);

        if (props) {
            const propsEl = document.createElement("div");
            propsEl.className = "node-props";
            Object.keys(props).forEach(function (key) {
                const span = document.createElement("span");
                span.textContent = key + ": " + props[key];
                propsEl.appendChild(span);
            });
            if (propsEl.children.length > 0) {
                node.appendChild(propsEl);
            }
        }

        return node;
    }

    function createArrow() {
        const arrow = document.createElement("span");
        arrow.className = "arrow";
        arrow.textContent = "\u2192";
        return arrow;
    }

    // ---- WebSocket live status ----

    function connectWebSocket() {
        const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
        ws = new WebSocket(proto + "//" + window.location.host + "/ws");

        ws.onmessage = function (event) {
            const status = JSON.parse(event.data);
            updateStatus(status);
        };

        ws.onclose = function () {
            setTimeout(connectWebSocket, 2000);
        };

        ws.onerror = function () {
            ws.close();
        };
    }

    function updateStatus(s) {
        var dot = document.getElementById("status-indicator");
        var text = document.getElementById("status-text");
        var uptime = document.getElementById("uptime");

        if (s.running) {
            dot.className = "status-dot running";
            text.textContent = "Running";
        } else {
            dot.className = "status-dot stopped";
            text.textContent = "Stopped";
        }

        uptime.textContent = s.uptime ? "uptime: " + s.uptime : "";

        document.getElementById("records-in").textContent = s.records_in || 0;
        document.getElementById("records-out").textContent = s.records_out || 0;

        var errDisplay = document.getElementById("error-display");
        var errText = document.getElementById("error-text");
        if (s.error) {
            errDisplay.classList.remove("hidden");
            errText.textContent = s.error;
        } else {
            errDisplay.classList.add("hidden");
        }
    }

    // ---- Init ----

    loadPipeline();
    connectWebSocket();
})();
