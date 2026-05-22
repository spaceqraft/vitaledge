package lexer

func lexEdge(tokens []Token) []Token {
	tokensOut := []Token{
		{Type: EDGE_START, Value: tokens[0].Value},
	}

	for left := 0; left < len(tokens); left++ {
		if tokens[left].Type == PUNCTUATION && tokens[left].Value == ":" {
			if tokens[left-1].Type == IDENTIFIER {
				tokensOut = append(tokensOut, []Token{{Type: EDGE_LABEL, Value: tokens[left-1].Value}}...)
			}
			if tokens[left+1].Type == IDENTIFIER {
				tokensOut = append(tokensOut, []Token{{Type: EDGE_TYPE, Value: tokens[left+1].Value}}...)
				left++
			}
		}

		if left < len(tokens) && tokens[left].Type == PUNCTUATION && tokens[left].Value == "{" {
			right := left
			for ; right < len(tokens); right++ {
				if tokens[right].Type == PUNCTUATION && tokens[right].Value == "}" {
					break
				}
			}

			propertyTokens := lexProperties(tokens[left:right])
			if len(propertyTokens) > 0 {
				rhs := tokens[right+2:]
				tokensOut = append(tokensOut[:left-1], propertyTokens...)
				tokensOut = append(tokensOut, rhs...)
			}
			left += len(propertyTokens) - 1
		}
	}

	tokensOut = append(tokensOut, Token{Type: EDGE_END, Value: tokens[len(tokens)-1].Value})
	return tokensOut
}
