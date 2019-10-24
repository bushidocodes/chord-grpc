async function loadData(){
    const response = await fetch("./data");
    const myJson = await response.json();
    
    const nodes = Object.values(myJson)
    .map(({id, ip, port}) =>({
        id, label: `${id} on ${ip}:${port}`
    }));
    
    const edges = Object.values(myJson)
        .filter(elem => !isNaN(elem.id) && elem.successor && !isNaN(elem.successor.id))
        .map(({id, successor}) =>({
            from: id, to: successor.id
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
            randomSeed: 0,
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
    }, 100)
    
}

document.addEventListener("DOMContentLoaded", function(){
    loadData();
    setInterval(()=> {
        loadData();
    }, 3000)
});