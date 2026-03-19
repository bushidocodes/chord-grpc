const CRAWLER_INTERVAL_MS = 1000;
const HASH_SPACE = 2 ** 32;

let network;
const nodeSet = new vis.DataSet();
const edgeSet = new vis.DataSet();

function hashCode(str) {
  let hash = 0;
  if (str.length === 0) return hash;
  for (let i = 0; i < str.length; i++) {
    const chr = str.charCodeAt(i);
    hash = (hash << 5) - hash + chr;
    hash |= 0;
  }
  return hash;
}

function computeCircularPositions(nodes, radius) {
  const sorted = [...nodes].sort((a, b) => a.data.id - b.data.id);
  const positions = {};
  sorted.forEach((node, i) => {
    // Place node evenly around the circle by rank order
    // Angle 0 is at the top, increasing clockwise
    const angle = (i / sorted.length) * 2 * Math.PI - Math.PI / 2;
    positions[node.id] = {
      x: Math.cos(angle) * radius,
      y: Math.sin(angle) * radius,
    };
  });
  return positions;
}

async function loadData() {
  const response = await fetch("./data");
  const myJson = await response.json();

  const container = document.getElementById("mynetwork");
  const radius = Math.min(container.clientWidth, container.clientHeight) * 0.35;

  const nodes = Object.values(myJson).map(({ id, host, port, ...rest }) => ({
    id: `${host}:${port}`,
    label: `${id}\n${host}:${port}`,
    data: { id, host, port, ...rest },
  }));

  const positions = computeCircularPositions(nodes, radius);

  // Apply fixed positions to nodes
  nodes.forEach((node) => {
    if (positions[node.id]) {
      node.x = positions[node.id].x;
      node.y = positions[node.id].y;
      node.fixed = { x: true, y: true };
    }
  });

  document.getElementById("nodeCount").innerText = `${nodes.length}`;

  // Successor edges (solid, follow the ring perimeter)
  const successorEdges = Object.values(myJson)
    .filter(
      (elem) =>
        elem.host &&
        elem.port &&
        elem.successor &&
        elem.successor.host &&
        elem.successor.port,
    )
    .map(({ host, port, successor }) => {
      const hash = hashCode(
        `succ:${host}:${port}-${successor.host}:${successor.port}`,
      );
      return {
        from: `${host}:${port}`,
        to: `${successor.host}:${successor.port}`,
        id: hash,
        color: { color: "#58a6ff", highlight: "#79c0ff", hover: "#79c0ff" },
        width: 2,
        smooth: { type: "curvedCW", roundness: 0.15 },
      };
    });

  // Build a map of node -> successor for filtering
  const successorMap = {};
  Object.values(myJson).forEach((elem) => {
    if (elem.host && elem.port && elem.successor) {
      successorMap[`${elem.host}:${elem.port}`] =
        `${elem.successor.host}:${elem.successor.port}`;
    }
  });

  // Finger table edges (dashed, cut across the ring)
  // Filter out fingers that point to the immediate successor
  const fingerEdges = Object.values(myJson)
    .filter((elem) => elem.host && elem.port && elem.fingerTable)
    .flatMap(({ host, port, fingerTable }) => {
      const fromId = `${host}:${port}`;
      const succId = successorMap[fromId];
      return Object.values(fingerTable)
        .filter((v) => v.host && v.port)
        .filter((v) => v.host !== host || v.port !== port)
        .filter((v) => `${v.host}:${v.port}` !== succId)
        .map((v) => {
          const hash = hashCode(`finger:${host}:${port}-${v.host}:${v.port}`);
          return {
            from: fromId,
            to: `${v.host}:${v.port}`,
            id: hash,
            dashes: [5, 8],
            color: { color: "#1f3d5c", highlight: "#58a6ff", hover: "#30536e" },
            width: 1,
            arrows: { to: { enabled: true, scaleFactor: 0.4 } },
            smooth: false,
          };
        });
    })
    .filter((edge, i, arr) => arr.findIndex((e) => e.id === edge.id) === i);

  const edges = [...successorEdges, ...fingerEdges];

  const updatedNodes = nodeSet.update(nodes);
  const allNodes = nodeSet.getIds();
  const nodesToRemove = _.difference(allNodes, updatedNodes);
  nodesToRemove.forEach((val) => nodeSet.remove(val));

  const updatedEdges = edgeSet.update(edges);
  edgeSet.forEach((val, idx) => {
    if (!updatedEdges.includes(idx)) {
      edgeSet.remove(idx);
    }
  });

  const data = { nodes: nodeSet, edges: edgeSet };
  const options = {
    autoResize: true,
    nodes: {
      shape: "box",
      font: {
        size: 14,
        face: "'SFMono-Regular', 'Cascadia Code', 'Fira Code', monospace",
        color: "#e1e4e8",
        multi: true,
      },
      color: {
        background: "#21262d",
        border: "#30363d",
        highlight: {
          background: "#1f6feb",
          border: "#58a6ff",
        },
        hover: {
          background: "#30363d",
          border: "#58a6ff",
        },
      },
      borderWidth: 1,
      borderWidthSelected: 2,
      margin: 10,
    },
    edges: {
      arrows: { to: { enabled: true, scaleFactor: 0.6 } },
      color: {
        color: "#30363d",
        highlight: "#58a6ff",
        hover: "#484f58",
      },
      width: 1.5,
      smooth: {
        type: "curvedCW",
        roundness: 0.15,
      },
    },
    physics: {
      enabled: false,
    },
    interaction: {
      hover: true,
      dragNodes: false,
    },
  };

  if (!network) {
    network = new vis.Network(container, data, options);
    network.on("click", function (params) {
      if (params.nodes.length > 0) {
        const nodeContent = document.getElementById("nodeContent");
        const data = nodeSet.get(params.nodes[0]).data;

        let html = "";

        // Node header
        html += `<div class="node-id">${data.id}</div>`;
        html += `<div class="node-host">${data.host}:${data.port}</div>`;

        // Predecessor
        html += `<div class="section-title">Predecessor</div>`;
        if (data.predecessor && data.predecessor.id) {
          html += `<div class="detail-row">`;
          html += `<span class="detail-key">${data.predecessor.id}</span>`;
          html += `<span class="detail-value">${data.predecessor.host}:${data.predecessor.port}</span>`;
          html += `</div>`;
        } else {
          html += `<span class="detail-key">None</span>`;
        }

        // Finger Table
        html += `<div class="section-title">Finger Table</div>`;
        if (data.fingerTable) {
          const entries = Object.entries(data.fingerTable);
          const above = entries.filter(([k]) => k >= data.id);
          const below = entries.filter(([k]) => k < data.id);
          [...above, ...below].forEach(([k, v]) => {
            html += `<div class="finger-entry">`;
            html += `<span class="finger-key">${k}</span>`;
            html += `<span class="finger-value">${v.id} @ ${v.host}:${v.port}</span>`;
            html += `</div>`;
          });
        }

        // Users
        html += `<div class="section-title">Users (${data.userIds ? data.userIds.length : 0})</div>`;
        if (data.userIds) {
          data.userIds.forEach(
            ({
              id,
              metadata: { primaryHash, secondaryHash, isPrimaryHash },
            }) => {
              const key = isPrimaryHash ? primaryHash : secondaryHash;
              const alternateKey = !isPrimaryHash ? primaryHash : secondaryHash;
              html += `<div class="user-entry">`;
              html += `<span class="user-primary">${key}</span> `;
              html += `User ${id} `;
              html += `<span class="user-secondary">(alt: ${alternateKey})</span>`;
              html += `</div>`;
            },
          );
        }

        nodeContent.innerHTML = html;
      }
    });
  } else {
    // Reposition nodes on subsequent updates (new nodes joining)
    nodes.forEach((node) => {
      if (positions[node.id]) {
        network.moveNode(node.id, positions[node.id].x, positions[node.id].y);
      }
    });
  }
}

document.addEventListener("DOMContentLoaded", function () {
  loadData();
  setInterval(() => {
    loadData();
  }, CRAWLER_INTERVAL_MS);
});
