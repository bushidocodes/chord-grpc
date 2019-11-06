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
  var network = new vis.Network(container, data, options);
  setTimeout(() => {
    network.redraw();
  }, 100);
}

document.addEventListener("DOMContentLoaded", function() {
  loadData();
  setInterval(() => {
    loadData();
  }, 3000);
});
