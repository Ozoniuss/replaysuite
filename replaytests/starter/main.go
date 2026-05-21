package main

import (
	"context"
	"log"

	"github.com/Ozoniuss/replaysuite/replaytests"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
)

// Debug against a real server to see if the interceptor implementation fails
// to detect certain NDEs.

func main() {
	// The client is a heavyweight object that should be created once per process.
	c, err := client.Dial(envconfig.MustLoadDefaultClientOptions())
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	opts := client.StartWorkflowOptions{
		ID:        "id",
		TaskQueue: "q",
	}

	we, err := c.ExecuteWorkflow(context.Background(), opts, replaytests.WorkflowWithInputChangeActivity)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}

	log.Println("Started workflow", "WorkflowID", we.GetID(), "RunID", we.GetRunID())

	// Synchronously wait for the workflow completion.
	var result string
	err = we.Get(context.Background(), &result)
	if err != nil {
		log.Fatalln("Unable get workflow result", err)
	}
	log.Println("Workflow result:", result)
}
