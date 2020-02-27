/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package example

import (
	"fmt"
	"log"

	nebula "github.com/vesoft-inc/nebula-go"
	graph "github.com/vesoft-inc/nebula-go/nebula/graph"
)

const (
	address  = "127.0.0.1"
	port     = 6699
	username = "user"
	password = "password"
)

type edge struct {
	start graph.IdType
	end   graph.IdType
}

func ksteps(client *nebula.GraphClient, k int, start graph.IdType) ([]graph.Idtype, []edge) {
	var vertices []graph.IdType
	var edges []edge
	nextSteps := []graph.IdType{start}
	for i := 0; i < k; i++ {
		var idList []graph.IdType
		for n := range nextSteps {
			resp, err := client.Execute(fmt.Sprintf("GO FROM %d OVER follow YIELD follow._dst", n))
			if err != nil {
				log.Print(err)
				return vertices, edges
			}
			if resp.GetErrorCode() != graph.ErrorCode_SUCCEEDED {
				t.Logf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, resp.GetErrorCode(), resp.GetErrorMsg())
				return vertices, edges
			}
			if !resp.IsSetRows() {
				return vertices, edges
			}
			for _, row := range resp.GetRows() {
				columns := row.GetColumns()
				id := columns[0].GetId()
				edges = append(edges, edge{
					start: n,
					end:   id,
				})
				idList = append(idList, id)
				vertices = append(vertices, id)
			}
		}
		nextSteps = idList
	}
	return vertices, edges
}

func main() {

	client, err := nebula.NewClient(fmt.Sprintf("%s:%d", address, port))
	if err != nil {
		t.Errorf("Fail to create client, address: %s, port: %d, %s", address, port, err.Error())
	}

	if err = client.Connect(username, password); err != nil {
		t.Errorf("Fail to connect server, username: %s, password: %s, %s", username, password, err.Error())
	}

	defer client.Disconnect()

	var k int = 5
	var start graph.IdType = 100

	vertices, edges = ksteps(client, k, start)
	log.Printf("vert: %d, edges: %d", len(vertices), len(edges))
}
