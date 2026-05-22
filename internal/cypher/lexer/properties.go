package lexer

func lexProperties(tokens []Token) []Token {
	propertyTokens := []Token{{Type: PROPERTY_START, Value: "{"}}

	for i := 0; i < len(tokens)-2; i++ {
		if tokens[i].Type == PUNCTUATION && tokens[i].Value == "}" {
			break
		} else if tokens[i].Type == PUNCTUATION && tokens[i].Value == "," {
			// skip comma
		} else if tokens[i].Type == IDENTIFIER &&
			tokens[i+1].Type == PUNCTUATION && tokens[i+1].Value == ":" {
			propertyTokens = append(propertyTokens, []Token{{Type: PROPERTY_KEY, Value: tokens[i].Value}, {Type: PROPERTY_VALUE, Value: tokens[i+2].Value}}...)
		}
	}

	propertyTokens = append(propertyTokens, Token{Type: PROPERTY_END, Value: "}"})
	return propertyTokens
}
