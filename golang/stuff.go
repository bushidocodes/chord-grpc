/* from testbench*/
/*
Testbench for implementation of the Chord protocol from the Stoica paper of 2001.

%%%%%    %%%%%%%%%%    %%%%%%%%%%    %%%%%
%%%%%    Revision:    20191013
%%%%%    %%%%%%%%%%    %%%%%%%%%%    %%%%%
*/

/*
%%%%%%%%%%
REVISION HISTORY

%%%     20191013
        Modified:   
    -added weird stub for eventual RPC comms

%%%     20191012
        Modified:   
    -moved chord definitions and methods onto their own package

%%%     20191011
        Author: Group 1    
        Project:    CSCI_6421_10_F19_Project1
    -Initial release

%%%%%%%%%%
*/
package main


//IMPORTED PACKAGES
import (
	"chord"
	"fmt"
	"strconv"
	"net"
	"runtime"
	"time"
)


//CONSTANTS
// default length in bits of the node identifiers; i.e., the 'm' value from section 4.2
const DEFAULT_IDENTIFIER_BIT_LENGTH int = 3


//FUNCTIONS
func parse_IPv4_address_string(ipv4_address_string string) net.IP {
	var golang_ipv4_address net.IP
	golang_ipv4_address = net.ParseIP(ipv4_address_string).To4()
	if golang_ipv4_address == nil {
		pc, _, line, _ := runtime.Caller(1)
		fmt.Printf("Error in [%s:%d]: Bad IP address string \"%s\"\n", 
			runtime.FuncForPC(pc).Name(), line, ipv4_address_string)
		return nil
	}
	return golang_ipv4_address
}


//BEGIN
func main() {
	var id_bit_length int = DEFAULT_IDENTIFIER_BIT_LENGTH
	var chord_node_ipv4_address_IP net.IP
	var chord_node_ipv4_address_string string = "127.0.0.1"
	var chord_node_id chord.ChordNodeId
	// convert IP address to native type
	chord_node_ipv4_address_IP = parse_IPv4_address_string(chord_node_ipv4_address_string)
	// create figure 3.b nodes
	var figure_3b_nodes = []chord.ChordNodeId{0, 1, 3}
	var chord_nodes []chord.ChordNode = make([]chord.ChordNode, 3)
	for i, v := range figure_3b_nodes {
		chord_nodes[i].Create_Node_IP(chord_node_ipv4_address_IP, v, id_bit_length)
		fmt.Println("Node created: ", chord_nodes[i].Id, " with fingers: ", chord_nodes[i].Finger_Table)
	}
	var rpc_stub = chord.RPCStub{figure_3b_nodes, chord_nodes}
	// launch a couple of nodes
	go chord_nodes[0].Launch_Node(1000, 5, rpc_stub)
	go chord_nodes[1].Launch_Node(1000, 5, rpc_stub)
	
	// create a "bad" node
	var chord_node_bad chord.ChordNode
	chord_node_id = 9
	chord_node_bad.Create_Node_IP(chord_node_ipv4_address_IP, chord_node_id, id_bit_length)
	fmt.Println("Node created: ", chord_node_bad.Id, ," with fingers: ", chord_node_bad.Finger_Table)
	// wait a bit
	for i := 0; i < 5; i++ {
		time.Sleep(1000 * time.Millisecond)
		fmt.Println("Waiting in main ", strconv.Itoa(i), " ...")
		if i == 2 {
			chord_nodes[1].Join(chord_nodes[0], rpc_stub)
		}
	}
}


/* from chord */
/*
Project 1

Implementation of the Chord protocol from the Stoica paper of 2001.

Reference: Stoica et al. "Chord: A Scalable Peer-to-peer Lookup Service for Internet
Applications". SIGCOMM'01, August 27-31, 2001, San Diego, California, USA. ACM 1-58113-411-8/01/00.

%%%%%    %%%%%%%%%%    %%%%%%%%%%    %%%%%
%%%%%    Revision:    20191013
%%%%%    %%%%%%%%%%    %%%%%%%%%%    %%%%%
*/
/*
%%%%%%%%%%
REVISION HISTORY

%%%     20191013
        Modified:   
    -added weird stub for eventual RPC comms

%%%     20191012
        Author: Group 1    
        Project:    CSCI_6421_10_F19_Project1
    -Initial release

%%%%%%%%%%
*/
package chord


//IMPORTED PACKAGES
import (
	"net"
	"fmt"
	"runtime"
	"time"
)


//CONSTANTS
// default length in bits of the node identifiers; i.e., the 'm' value from section 4.2
const DEFAULT_IDENTIFIER_BIT_LENGTH int = 3


//FUNCTIONS
type ChordNode struct {
	Id		ChordNodeId
	Address		ChordNodeAddress
	Predecessor	ChordNodeFinger
	Finger_Table	[]ChordNodeFinger
	Node_RPC_Hooks	NodeRPCHooks
}

type ChordNodeId int

type ChordNodeAddress struct {
	address	net.IP
	port	int
}

type ChordNodeFinger struct {
	successor	ChordNodeId	
}

const FIND_SUCCESSOR = 1
const SUCCESSOR_FOUND = FIND_SUCCESSOR
const FIND_PREDECESSOR = 2
const PREDECESSOR_FOUND = FIND_PREDECESSOR
type NodeRPCHooks struct {
	In_bound	chan NodeRPCHook
	Out_bound	chan NodeRPCHook
}
type NodeRPCHook struct {
	Command	int
	Data	ChordNodeId
}
func (chord_node *ChordNode) initialize_rpc_hooks() {
	chord_node.Node_RPC_Hooks.In_bound = make(chan NodeRPCHook)
	chord_node.Node_RPC_Hooks.Out_bound = make(chan NodeRPCHook)
}

func (chord_node *ChordNode) create_finger_table(id_bit_length int) int {
	var next_finger_id ChordNodeId
	chord_node.Finger_Table = make([]ChordNodeFinger, id_bit_length)
	for i := 1; i <= id_bit_length; i++ {
		next_finger_id = chord_node.Id
		chord_node.Finger_Table[i - 1].successor = next_finger_id
	}
	return len(chord_node.Finger_Table)
}

func (chord_node ChordNode) find_successor(key_in ChordNodeId, 
		existing_chord_node_id ChordNodeId, rpc_stub RPCStub) ChordNodeId {
	var successor_query NodeRPCHook
	// the for-loop picks the specified node out of the stub lineup
	for i, v := range rpc_stub.Chord_node_ids {
		if v == existing_chord_node_id {
			//(from figure 4) n'.find_predecessor(id)
			successor_query = NodeRPCHook{Command: FIND_PREDECESSOR, Data: key_in}
			rpc_stub.Chord_nodes[i].Node_RPC_Hooks.In_bound <- successor_query
		}
	}
	return key_in
}

func (chord_node ChordNode) find_predecessor(key_in ChordNodeId) ChordNodeId {
	for i := 1; i < len(chord_node.Finger_Table); i++ {
		/* TBD / fmt.Printf("i = %d; v = %d; key = %d\n", i, chord_node.Finger_Table[i], key_in) /* TBD */
		if key_in <= chord_node.Finger_Table[i].successor {
			return key_in
		}
	}
	return chord_node.Id
}

/*func (chord_node ChordNode) closest_preceding_node(key_in ChordNodeId) ChordNodeId {
	for i := len(chord_node.Finger_Table[i].successor - 1; i >= 0; i-- {
		if chord_node.Finger_Table[i].successor < chord_node.Id && 
			chord_node.Finger_Table[i].successor > key_in {
			return chord_node.Finger_Table[i].successor
		}
	}
	return chord_node.Id
}*/

func (chord_node *ChordNode) Create_Node_IP(chord_node_address net.IP, node_id ChordNodeId, id_bit_length int) {
	/* initialize a node's data structures, taking a golang native IP */
	chord_node.Address.address = chord_node_address
	chord_node.Id = node_id
	chord_node.create_finger_table(id_bit_length)
	chord_node.initialize_rpc_hooks()
}

type RPCStub struct {
	// temporary type to make temporary RPC stub less unreadable
	Chord_node_ids	[]ChordNodeId
	Chord_nodes	[]ChordNode
}

func (chord_node *ChordNode) Launch_Node(interval_ms, lifespan_s int, rpc_stub RPCStub) {
	//var node_RPC_hooks NodeRPCHooks
	//node_RPC_hooks.In_bound = make(chan NodeRPCHook)
	//node_RPC_hooks.Out_bound = make(chan NodeRPCHook)
	//var node_RPC_hook_in, node_RPC_hook_out NodeRPCHook
	/* launch a node's thread */
	if lifespan_s * 1000 <= interval_ms {
		pc, _, line, _ := runtime.Caller(1)
		fmt.Printf("Error in [%s:%d]: Interval of %d milliseconds seems too large ", 
			"relative to the life span of %d seconds\n", 
			runtime.FuncForPC(pc).Name(), line, interval_ms, lifespan_s)
		return
	}
	tick := time.Tick(time.Duration(interval_ms) * time.Millisecond)
	boom := time.After(time.Duration(lifespan_s) * time.Second)
	for {
		select {
		case node_RPC_hook_in := <- chord_node.Node_RPC_Hooks.In_bound:
			fmt.Printf("[info] Message received: {%d}\n", node_RPC_hook_in)
			switch {
			case node_RPC_hook_in.Command == FIND_SUCCESSOR:
				fmt.Printf("[info] find_successor(%d)\n", node_RPC_hook_in.Data)
			case node_RPC_hook_in.Command == FIND_PREDECESSOR:
				
			default:
				fmt.Printf("[warning] Unkown command received: %d\n", node_RPC_hook_in.Command)
			}
		case <- tick:
			//fmt.Printf("[info] Chord alive %d\n", chord_node.Id)
		case <- boom:
			fmt.Printf("[warning] Chord dead %d\n", chord_node.Id)
			return
		default:
			//time.Sleep(1 * time.Millisecond)
		}
	}
}

func (chord_node *ChordNode) Join(existing_chord_node ChordNode, rpc_stub RPCStub) {
	//chord_node.find_successor(chord_node.Id + 1, existing_chord_node.Id, rpc_stub)
	var successor_query NodeRPCHook
	// n'.find_successor(finger[1].start)
	// the for-loop picks the specified node out of the stub lineup
	for i, v := range rpc_stub.Chord_node_ids {
		if v == existing_chord_node.Id {
			successor_query = NodeRPCHook{Command: FIND_SUCCESSOR, Data: chord_node.Id + 1}
			rpc_stub.Chord_nodes[i].Node_RPC_Hooks.In_bound <- successor_query
		}
	}		
}
