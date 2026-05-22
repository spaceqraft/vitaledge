package scanner

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
)
