package models

import "time"

// EventKind represents the type of AT Protocol event coming from the firehose.
type EventKind string

const (
	EventPost    EventKind = "app.bsky.feed.post"
	EventRepost  EventKind = "app.bsky.feed.repost"
	EventFollow  EventKind = "app.bsky.graph.follow"
	EventProfile EventKind = "app.bsky.actor.profile"
)

type FirehoseEvent struct {
	ID         string    `json:"id"`
	DID        string    `json:"did"` // author decentralized identifier
	Kind       EventKind `json:"kind"`
	Text       string    `json:"text"` // post body (empty for non-post events)
	Langs      []string  `json:"langs"`
	Links      []string  `json:"links"` // URLs found in the post body
	CreatedAt  time.Time `json:"created_at"`
	ReceivedAt time.Time `json:"received_at"`
}
