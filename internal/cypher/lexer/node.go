package lexer

func lexNode(tokens []Token) []Token {
	tokensOut := []Token{
		{Type: NODE_START, Value: "("},
	}

	left := 1
	if left < len(tokens) && tokens[left].Type == IDENTIFIER {
		tokensOut = append(tokensOut, []Token{{Type: NODE_LABEL, Value: tokens[left].Value}}...)
		left++
	}

	if left < len(tokens) && tokens[left].Type == PUNCTUATION && tokens[left].Value == ":" {
		left++
		for ; left < len(tokens); left++ {
			if tokens[left].Type == PUNCTUATION && tokens[left].Value == "{" {
				break
			} else if tokens[left].Type == PUNCTUATION && tokens[left].Value == "!" && tokens[left+1].Type == IDENTIFIER {
				tokensOut = append(tokensOut, []Token{{Type: NODE_NOT_TYPE, Value: tokens[left+1].Value}}...)
				left++
			} else if tokens[left].Type == IDENTIFIER {
				tokensOut = append(tokensOut, []Token{{Type: NODE_TYPE, Value: tokens[left].Value}}...)
			}
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

	tokensOut = append(tokensOut, []Token{{Type: NODE_END, Value: ")"}}...)
	return tokensOut
}
