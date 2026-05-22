package lexer

func Lex(tokens []Token) ([]Token, error) {
	tokensOut := []Token{}

	for left := 0; left < len(tokens); left++ {
		token := tokens[left]
		if token.Type == PUNCTUATION && (token.Value == "(") {
			for right := left + 1; right < len(tokens); right++ {
				if tokens[right].Type == PUNCTUATION && tokens[right].Value == ")" {
					mergedValue := lexNode(tokens[left : right+1])
					tokensOut = append(tokensOut, mergedValue...)
					left = right
					break
				}
			}
		}

		if token.Type == PUNCTUATION && (token.Value == "-" || token.Value == "<") {
			for right := left + 1; right < len(tokens); right++ {
				if tokens[right].Type == PUNCTUATION && tokens[right].Value == "(" {
					right--
					mergedValue := lexEdge(tokens[left : right+1])
					tokensOut = append(tokensOut, mergedValue...)
					left = right
					break
				}
			}
		}
	}

	return tokensOut, nil
}
