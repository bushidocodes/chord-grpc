let network;
let oldData;

// The dataviz library assigns the edges UIDs that we want to ignore when comparing with lodash
function isEqual(oldData, newData) {
  const oldDataWithoutEdgeIds = {
    nodes: oldData.nodes,
    edges: oldData.edges.map(({ from, to }) => ({ from, to }))
  };
  const newDataWithoutEdgeIds = {
    nodes: newData.nodes,
    edges: newData.edges.map(({ from, to }) => ({ from, to }))
  };
  return _.isEqual(oldDataWithoutEdgeIds, newDataWithoutEdgeIds);
}

async function loadData() {
  const response = await fetch("./data");
  const myJson = await response.json();

  const nodes = Object.values(myJson).map(({ id, ip, port }) => ({
    id: `${ip}:${port}`,
    label: `${id} on ${ip}:${port}`
  }));

  const edges = Object.values(myJson)
    .filter(
      elem =>
        elem.ip &&
        elem.port &&
        elem.successor &&
        elem.successor.ip &&
        elem.successor.port
    )
    .map(({ ip, port, successor }) => ({
      from: `${ip}:${port}`,
      to: `${successor.ip}:${successor.port}`
    }));
  // create a network
  var container = document.getElementById("mynetwork");
  var data = {
    nodes: nodes,
    edges: edges
  };
  var options = {
    autoResize: true,
    layout: {
      randomSeed: 0
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
    oldData = data;
  }

  if (!isEqual(data, oldData)) {
    network.setData(data);
    oldData = data;
  }
}

document.addEventListener("DOMContentLoaded", function() {
  loadData();
  setInterval(() => {
    loadData();
  }, 3000);
});
