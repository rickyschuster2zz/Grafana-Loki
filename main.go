package main

import (
	"errors"
	"fmt"
	"sync"
)

// CommitFailedException is returned when a commit attempt is made for a partition
// that is no longer owned by the consumer or if the generation ID has changed.
type CommitFailedException struct {
	Message string
}

func (e CommitFailedException) Error() string {
	return e.Message
}

// ErrCommitFailed is a convenience error value for CommitFailedException.
var ErrCommitFailed = CommitFailedException{
	Message: "CommitFailedException: commit failed because the consumer is no longer the owner of the partition or the generation has changed",
}

// Consumer represents a Kafka consumer client that manages partition ownership and offset commits.
type Consumer struct {
	mu           sync.RWMutex
	generationID int
	partitions   map[int]bool
}

// NewConsumer creates and initializes a new Consumer.
func NewConsumer() *Consumer {
	return &Consumer{
		partitions:   make(map[int]bool),
		generationID: -1, // -1 indicates no active generation
	}
}

// Assign assigns a set of partitions to the consumer for a specific generation.
func (c *Consumer) Assign(generationID int, partitions []int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generationID = generationID
	c.partitions = make(map[int]bool)
	for _, p := range partitions {
		c.partitions[p] = true
	}
}

// Revoke revokes all partitions from the consumer, resetting the generation.
func (c *Consumer) Revoke() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.partitions = make(map[int]bool)
	c.generationID = -1 // Invalidate generation ID on revocation
}

// Commit validates partition ownership and generation ID before committing offsets.
// If validation fails, it returns a CommitFailedException.
func (c *Consumer) Commit(partition int, offset int64, generationID int) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Validate generation ID
	if c.generationID == -1 || c.generationID != generationID {
		return ErrCommitFailed
	}

	// Validate partition ownership
	if !c.partitions[partition] {
		return ErrCommitFailed
	}

	// Commit logic would go here (e.g., sending request to coordinator)
	return nil
}

// CommitOffset is an alias for Commit to support different naming conventions.
func (c *Consumer) CommitOffset(partition int, offset int64, generationID int) error {
	return c.Commit(partition, offset, generationID)
}

func main() {
	// Run a quick demonstration of the fix
	consumer := NewConsumer()

	// 1. Assign partitions 0 and 1 for generation 1
	consumer.Assign(1, []int{0, 1})
	fmt.Println("Assigned generation 1, partitions [0, 1]")

	// 2. Commit should succeed
	err := consumer.Commit(0, 100, 1)
	if err != nil {
		fmt.Printf("Unexpected error: %v\n", err)
	} else {
		fmt.Println("Commit succeeded for partition 0, offset 100, generation 1")
	}

	// 3. Revoke partitions (e.g., during rebalance)
	consumer.Revoke()
	fmt.Println("Partitions revoked")

	// 4. Commit attempt post-revocation should fail
	err = consumer.Commit(0, 101, 1)
	if err != nil {
		if errors.As(err, &CommitFailedException{}) {
			fmt.Printf("Commit failed as expected with CommitFailedException: %v\n", err)
		} else {
			fmt.Printf("Commit failed with unexpected error: %v\n", err)
		}
	} else {
		fmt.Println("Error: Commit succeeded post-revocation!")
	}
}