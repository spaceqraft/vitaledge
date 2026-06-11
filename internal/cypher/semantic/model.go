package semantic

import "github.com/paegun/vitaledge/internal/cypher/pipeline"

// Model is the semantic output consumed by logical planning.
type Model = pipeline.SemanticModel

// ProjectionIntent aliases pipeline-level projection intent.
type ProjectionIntent = pipeline.ProjectionIntent

// ProjectionItemIntent aliases pipeline-level projection item intent.
type ProjectionItemIntent = pipeline.ProjectionItemIntent

// OrderingIntent aliases pipeline-level ordering intent.
type OrderingIntent = pipeline.OrderingIntent

// PaginationIntent aliases pipeline-level pagination intent.
type PaginationIntent = pipeline.PaginationIntent

// PatternIntent aliases pipeline-level pattern intent.
type PatternIntent = pipeline.PatternIntent

// CallIntent aliases pipeline-level call intent.
type CallIntent = pipeline.CallIntent

// WriteActionIntent aliases pipeline-level write sequencing intent.
type WriteActionIntent = pipeline.WriteActionIntent
