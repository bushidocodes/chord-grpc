const CRAWLER_INTERVAL_MS = 1000;

let network;
const nodeSet = new vis.DataSet();
const edgeSet = new vis.DataSet();

function hashCode(str) {
  let hash = 0;
  if (str.length === 0) return hash;
  for (let i = 0; i < str.length; i++) {
    let chr = str.charCodeAt(i);
    hash = (hash << 5) - hash + chr;
    hash |= 0; // Convert to 32bit integer
  }
  return hash;
}

async function loadData() {
  const response = await fetch("./data");
  const myJson = await response.json();

  const nodes = Object.values(myJson).map(({ id, host, port, ...rest }) => ({
    id: `${host}:${port}`,
    label: `${id} on ${host}:${port}`,
    data: { id, host, port, ...rest }
  }));
  document.getElementById("nodeCount").innerText = `${nodes.length}`;

  const edges = Object.values(myJson)
    .filter(
      elem =>
        elem.host &&
        elem.port &&
        elem.successor &&
        elem.successor.host &&
        elem.successor.port
    )
    .map(({ host, port, successor }) => {
      let hash = hashCode(
        `${host}:${port}-${successor.host}:${successor.port}`
      );
      return {
        from: `${host}:${port}`,
        to: `${successor.host}:${successor.port}`,
        id: hash
      };
    });

  const updatedNodes = nodeSet.update(nodes);
  const allNodes = nodeSet.getIds();
  const nodesToRemove = _.difference(allNodes, updatedNodes);
  nodesToRemove.forEach(val => nodeSet.remove(val));

  const updatedEdges = edgeSet.update(edges);
  edgeSet.forEach((val, idx, arr) => {
    if (!updatedEdges.includes(idx)) {
      edgeSet.remove(idx);
    }
  });

  // create a network
  var container = document.getElementById("mynetwork");
  var data = {
    nodes: nodeSet,
    edges: edgeSet
  };
  var options = {
    autoResize: true,
    layout: {
      randomSeed: 30
    },
    nodes: {
      shape: "box",
      font: "24px arial"
    },
    physics: {
      enabled: true,
      repulsion: {
        nodeDistance: 300,
        springLength: 400
      },
      solver: "repulsion"
    }
  };
  if (!network) {
    network = new vis.Network(container, data, options);
    network.on("click", function(params) {
      if (params.nodes.length > 0) {
        var nodeContent = document.getElementById("nodeContent");
        var data = nodeSet.get(params.nodes[0]).data; // get the data from selected node
        const entries = Object.entries(data);
        let domString = "";
        entries
          .filter(([k, v]) => typeof v != "object")
          .forEach(([key, value]) => {
            domString = domString.concat(`${key}: ${value}<br>`);
          });
        domString = domString.concat(`<br>Successor: <br>`);
        if (data.successor) {
          domString = domString.concat(
            `${data.successor.id} @ ${data.successor.host}:${data.successor.port}<br>`
          );
        }

        domString = domString.concat(`<br>Finger Table: <br>`);
        if (data.fingerTable) {
          Object.entries(data.fingerTable)
            .filter(([k, v]) => k >= data.id)
            .forEach(([k, v]) => {
              domString = domString.concat(
                `${k} => ${v.id} @ ${v.host}:${v.port}<br>`
              );
            });
          Object.entries(data.fingerTable)
            .filter(([k, v]) => k < data.id)
            .forEach(([k, v]) => {
              domString = domString.concat(
                `${k} => ${v.id} @ ${v.host}:${v.port}<br>`
              );
            });
        }
        nodeContent.innerHTML = domString;
      }
    });
  }
}

document.addEventListener("DOMContentLoaded", function() {
  loadData();
  setInterval(() => {
    loadData();
  }, CRAWLER_INTERVAL_MS);
});
