package node_test

import (
	"github.com/arya-analytics/aspen/internal/address"
	"github.com/arya-analytics/aspen/internal/node"
	. "github.com/onsi/ginkgo/v2"
	log "github.com/sirupsen/logrus"
	"time"
)

var _ = Describe("Node", func() {
	It("Should gossip correctly", func() {
		router := node.Router{Transports: make(map[address.Address]*node.SyncTransport)}

		node1Addr := address.Address("localhost:1234")
		node2Addr := address.Address("localhost:1235")
		node3Addr := address.Address("localhost:1236")

		node1 := node.Node{
			ID:      1,
			Address: node1Addr,
		}
		node2 := node.Node{
			ID:      2,
			Address: node2Addr,
		}
		node3 := node.Node{
			ID:      3,
			Address: node3Addr,
		}

		node1Gossip := node.Gossip{
			PeerAddresses: []address.Address{node2Addr},
			NodeID:        node1.ID,
			Nodes:         node.Nodes{node1.ID: node1},
			Transport:     router.New(node1.Address),
		}
		node2Gossip := node.Gossip{
			PeerAddresses: []address.Address{node3Addr},
			NodeID:        node2.ID,
			Nodes:         node.Nodes{node2.ID: node2},
			Transport:     router.New(node2.Address),
		}
		node3Gossip := node.Gossip{
			PeerAddresses: []address.Address{node1Addr},
			NodeID:        node3.ID,
			Nodes:         node.Nodes{node3.ID: node3},
			Transport:     router.New(node3.Address),
		}

		log.Info("INITIAL STATE NODE 2", node2Gossip.Nodes)
		log.Info("INITIAL STATE NODE 1", node1Gossip.Nodes)
		log.Info("INITIAL STATE NODE 3", node3Gossip.Nodes)

		errc := node1Gossip.Gossip()
		errC2 := node2Gossip.Gossip()
		errC3 := node3Gossip.Gossip()

		go func() {
			select {
			case err := <-errc:
				log.Error(err)
			case err := <-errC2:
				log.Error(err)
			case err := <-errC3:
				log.Error(err)
			}
		}()

		time.Sleep(500 * time.Millisecond)

		log.Info("FINAL STATE NODE 2", node2Gossip.Nodes)
		log.Info("FINAL STATE NODE 1", node1Gossip.Nodes)
		log.Info("FINAL STATE NODE 3", node3Gossip.Nodes)
	})
})
