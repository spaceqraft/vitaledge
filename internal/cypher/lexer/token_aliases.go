package lexer

import base "github.com/paegun/vitaledge/internal/cypher"

type Token = base.Token

type TokenType = base.TokenType

const (
	KEYWORD       = base.KEYWORD
	IDENTIFIER    = base.IDENTIFIER
	NUMBER        = base.NUMBER
	STRING        = base.STRING
	PUNCTUATION   = base.PUNCTUATION
	NEGATIVE_SIGN = base.NEGATIVE_SIGN

	NODE_START     = base.NODE_START
	NODE_END       = base.NODE_END
	NODE_LABEL     = base.NODE_LABEL
	NODE_TYPE      = base.NODE_TYPE
	NODE_NOT_TYPE  = base.NODE_NOT_TYPE
	PROPERTY_START = base.PROPERTY_START
	PROPERTY_KEY   = base.PROPERTY_KEY
	PROPERTY_VALUE = base.PROPERTY_VALUE
	PROPERTY_END   = base.PROPERTY_END
	EDGE_START     = base.EDGE_START
	EDGE_LABEL     = base.EDGE_LABEL
	EDGE_TYPE      = base.EDGE_TYPE
	EDGE_END       = base.EDGE_END
)
