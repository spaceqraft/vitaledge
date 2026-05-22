package scanner

import (
	"fmt"
)

var whitespace = map[rune]bool{
	' ':  true,
	'\t': true,
	'\n': true,
	'\r': true,

	'\u2029': true, // Paragraph separator
	'\u1680': true, // Ogham space mark
	'\u2000': true, // En quad
	'\u2001': true, // Em quad
	'\u2002': true, // En space
	'\u2003': true, // Em space
	'\u2004': true, // Three-per-em space
	'\u2005': true, // Four-per-em space
	'\u2006': true, // Six-per-em space
	'\u2007': true, // Figure space
	'\u2008': true, // Punctuation space
	'\u2009': true, // Thin space
	'\u200A': true, // Hair space
	'\u202F': true, // Narrow no-break space
	'\u205F': true, // Medium mathematical space
	'\u3000': true, // Ideographic space

	'\u2028': true, // Line separator
	'\u000B': true, // Vertical tab
	'\u000C': true, // Form feed

	'\u001C': true, // File separator
	'\u001D': true, // Group separator
	'\u001E': true, // Record separator
	'\u001F': true, // Unit separator

	'\u0085': true, // Next line
}

var keywords = map[string]bool{
	"ACCESS":             true,
	"ACTIVE":             true,
	"ADMIN":              true,
	"ADMINISTRATOR":      true,
	"ALIAS":              true,
	"ALIASES":            true,
	"ALL":                true,
	"ALL_SHORTEST_PATHS": true,
	"ALTER":              true,
	"AND":                true,
	"ANY":                true,
	"ARRAY":              true,
	"AS":                 true,
	"ASC":                true,
	"ASCENDING":          true,
	"ASSIGN":             true,
	"AT":                 true,
	"AUTH":               true,

	"BINDINGS": true,
	"BOOL":     true,
	"BOOLEAN":  true,
	"BOOSTED":  true,
	"BOTH":     true,
	"BREAK":    true,
	"BUILT":    true,
	"BY":       true,

	"CALL":        true,
	"CASCADE":     true,
	"CASE":        true,
	"CHANGE":      true,
	"CIDR":        true,
	"COLLECT":     true,
	"COMMAND":     true,
	"COMMANDS":    true,
	"COMPOSITE":   true,
	"CONCURRENT":  true,
	"CONSTRAINT":  true,
	"CONSTRAINTS": true,
	"CONTAINS":    true,
	"CONTINUE":    true,
	"COPY":        true,
	"COUNT":       true,
	"CREATE":      true,
	"CSV":         true,
	"CURRENT":     true,

	"DATA":       true,
	"DATABASE":   true,
	"DATABASES":  true,
	"DATE":       true,
	"DATETIME":   true,
	"DBMS":       true,
	"DEALLOCATE": true,
	"DEFAULT":    true,
	"DEFINED":    true,
	"DELETE":     true,
	"DENY":       true,
	"DESC":       true,
	"DESCENDING": true,
	"DESTROY":    true,
	"DETACH":     true,
	"DIFFERENT":  true,
	"DISTINCT":   true,
	"DRIVER":     true,
	"DROP":       true,
	"DRYRUN":     true,
	"DUMP":       true,
	"DURATION":   true,

	"EACH":       true,
	"EDGE":       true,
	"ELEMENT":    true,
	"ELSE":       true,
	"ENABLE":     true,
	"ENCRYPTED":  true,
	"END":        true,
	"ENDS":       true,
	"ERROR":      true,
	"EXECUTABLE": true,
	"EXECUTE":    true,
	"EXIST":      true,
	"EXISTENCE":  true,
	"EXISTS":     true,

	"FAIL":            true,
	"false":           true,
	"FIELDTERMINATOR": true,
	"FINISH":          true,
	"FILTER":          true,
	"FLOAT":           true,
	"FOR":             true,
	"FOREACH":         true,
	"FROM":            true,
	"FULLTEXT":        true,
	"FUNCTION":        true,
	"FUNCTIONS":       true,

	"GRANT":  true,
	"GRAPH":  true,
	"GRAPHS": true,
	"GROUP":  true,
	"GROUPS": true,

	"HEADERS": true,
	"HOME":    true,

	"ID":          true,
	"IF":          true,
	"IMMUTABLE":   true,
	"IMPERSONATE": true,
	"IN":          true,
	"INDEX":       true,
	"INDEXES":     true,
	"INF":         true,
	"INFINITY":    true,
	"INSERT":      true,
	"INT":         true,
	"INTEGER":     true,
	"IS":          true,

	"JOIN": true,

	"KEY": true,

	"LABEL":     true,
	"LABELS":    true,
	"LEADING":   true,
	"LET":       true,
	"LIMITROWS": true,
	"LIST":      true,
	"LOAD":      true,
	"LOCAL":     true,
	"LOOKUP":    true,

	"MANAGEMENT": true,
	"MAP":        true,
	"MATCH":      true,
	"MERGE":      true,

	"NAME":       true,
	"NAMES":      true,
	"NAN":        true,
	"NEW":        true,
	"NEXT":       true,
	"NFC":        true,
	"NFD":        true,
	"NFKC":       true,
	"NFKD":       true,
	"NODE":       true,
	"NODES":      true,
	"NODETACH":   true,
	"NONE":       true,
	"NORMALIZE":  true,
	"NORMALIZED": true,
	"NOT":        true,
	"NOTHING":    true,
	"NOWAIT":     true,
	"null":       true,

	"OF":       true,
	"OFFSET":   true,
	"ON":       true,
	"ONLY":     true,
	"OPTION":   true,
	"OPTIONAL": true,
	"OPTIONS":  true,
	"OR":       true,
	"ORDER":    true,

	"PASSWORD":   true,
	"PASSWORDS":  true,
	"PATH":       true,
	"PATHS":      true,
	"PLAINTEXT":  true,
	"POINT":      true,
	"POPULATED":  true,
	"PRIMARIES":  true,
	"PRIMARY":    true,
	"PRIVILEGE":  true,
	"PRIVILEGES": true,
	"PROCEDURE":  true,
	"PROCEDURES": true,
	"PROPERTIES": true,
	"PROPERTY":   true,
	"PROVIDER":   true,
	"PROVIDERS":  true,

	"RANGE":         true,
	"READ":          true,
	"REALLOCATE":    true,
	"REDUCE":        true,
	"REL":           true,
	"RELATIONSHIP":  true,
	"RELATIONSHIPS": true,
	"REMOVE":        true,
	"RENAME":        true,
	"REPEATABLE":    true,
	"REPLACE":       true,
	"REPORT":        true,
	"REQUIRE":       true,
	"REQUIRED":      true,
	"RESTRICT":      true,
	"RETURN":        true,
	"REVOKE":        true,
	"ROLE":          true,
	"ROLES":         true,
	"ROW":           true,
	"ROWS":          true,

	"SCAN":          true,
	"SCORE":         true,
	"SEARCH":        true,
	"SEC":           true,
	"SECOND":        true,
	"SECONDARIES":   true,
	"SECONDARY":     true,
	"SECONDS":       true,
	"SEEK":          true,
	"SERVER":        true,
	"SERVERS":       true,
	"SET":           true,
	"SETTING":       true,
	"SETTINGS":      true,
	"SHORTEST":      true,
	"SHORTEST_PATH": true,
	"SHOW":          true,
	"SIGNED":        true,
	"SINGLE":        true,
	"SKIPROWS":      true,
	"START":         true,
	"STARTS":        true,
	"STATUS":        true,
	"STOP":          true,
	"STRING":        true,
	"SUPPORTED":     true,
	"SUSPENDED":     true,

	"TARGET":       true,
	"TERMINATE":    true,
	"TEXT":         true,
	"THEN":         true,
	"TIME":         true,
	"TIMESTAMP":    true,
	"TIMEZONE":     true,
	"TO":           true,
	"TOPOLOGY":     true,
	"TRAILING":     true,
	"TRANSACTION":  true,
	"TRANSACTIONS": true,
	"TRAVERSE":     true,
	"TRIM":         true,
	"true":         true,
	"TYPE":         true,
	"TYPED":        true,
	"TYPES":        true,

	"UNION":      true,
	"UNIQUE":     true,
	"UNIQUENESS": true,
	"UNWIND":     true,
	"URL":        true,
	"USE":        true,
	"USER":       true,
	"USERS":      true,
	"USING":      true,

	"VALUE":   true,
	"VARCHAR": true,
	"VECTOR":  true,
	"VERTEX":  true,

	"WAIT":    true,
	"WHEN":    true,
	"WHERE":   true,
	"WITH":    true,
	"WITHOUT": true,
	"WRITE":   true,

	"XOR": true,

	"YIELD": true,

	"ZONE":  true,
	"ZONED": true,
}

func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

func isIdentifierStart(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_'
}

func isIdentifierPart(r rune) bool {
	return isIdentifierStart(r) || isDigit(r)
}

func isInfinityOrNaN(query string, pos int) bool {
	if pos+3 <= len(query) && query[pos:pos+3] == "Inf" {
		return true
	}
	if pos+8 <= len(query) && query[pos:pos+8] == "Infinity" {
		return true
	}
	if pos+3 <= len(query) && query[pos:pos+3] == "NaN" {
		return true
	}
	return false
}

func Scan(query string) ([]Token, error) {
	tokens := []Token{}

	right := 0
	for left := 0; left < len(query); left++ {
		// handle quoted strings
		if query[left] == '\'' || query[left] == '"' {
			token, err := extractQuotedString(query, left)
			if err != nil {
				// handle error (e.g., unterminated string)
				return nil, err
			}
			tokens = append(tokens, token)
			left += len(token.Value) - 1
		} else if whitespace[rune(query[left])] {
			// skip whitespace
		} else if query[left] == '-' {
			tokens = append(tokens, Token{
				Type:  NEGATIVE_SIGN,
				Value: query[left : left+1],
			})
		} else if isDigit(rune(query[left])) && (query[left+1] == 'x' || query[left+1] == 'X') {
			// handle hexadecimal numbers
			right = left + 2
			for right < len(query) && (isDigit(rune(query[right])) || (query[right] >= 'a' && query[right] <= 'f') || (query[right] >= 'A' && query[right] <= 'F') || query[right] == '_') {
				right++
			}
			tokens = append(tokens, Token{
				Type:  NUMBER,
				Value: query[left:right],
			})
			left = right - 1
		} else if isDigit(rune(query[left])) && (query[left+1] == 'o' || query[left+1] == 'O') {
			// handle octal numbers
			right = left + 2
			for right < len(query) && (isDigit(rune(query[right])) || query[right] == '_') {
				right++
			}
			tokens = append(tokens, Token{
				Type:  NUMBER,
				Value: query[left:right],
			})
			left = right - 1
		} else if isDigit(rune(query[left])) {
			// handle numbers
			right = left + 1
			for right < len(query) && (isDigit(rune(query[right])) || query[right] == '_' || query[right] == '.' || query[right] == 'e' || query[right] == 'E' || query[right] == '-' || query[right] == '+') {
				right++
			}
			tokens = append(tokens, Token{
				Type:  NUMBER,
				Value: query[left:right],
			})
			left = right - 1
		} else if isInfinityOrNaN(query, left) {
			right = left + 3
			if query[left] == 'I' {
				// check for "Infinity"
				if right+5 < len(query) && query[left:right+5] == "Infinity" {
					right += 5
				}
			}
			tokens = append(tokens, Token{
				Type:  NUMBER,
				Value: query[left:right],
			})
			left = right - 1
		} else if isIdentifierStart(rune(query[left])) {
			// handle identifiers and keywords
			right = left + 1
			for right < len(query) && isIdentifierPart(rune(query[right])) {
				right++
			}
			value := query[left:right]
			tokenType := IDENTIFIER
			if keywords[value] {
				tokenType = KEYWORD
			}
			tokens = append(tokens, Token{
				Type:  tokenType,
				Value: value,
			})
			left = right - 1
		} else {
			// handle punctuation
			tokens = append(tokens, Token{
				Type:  PUNCTUATION,
				Value: string(query[left : left+1]),
			})
		}
	}

	return tokens, nil
}

func extractQuotedString(query string, left int) (Token, error) {
	quoteChar := query[left]
	right := left + 1
	for right < len(query) {
		switch query[right] {
		case '\\':
			// skip escaped character
			right += 2
		case quoteChar:
			// end of string
			return Token{
				Type:  STRING,
				Value: query[left : right+1],
			}, nil
		default:
			right++
		}
	}
	return Token{}, fmt.Errorf("unterminated string literal at position %d", left)
}

func Keywords() []string {
	keys := make([]string, 0, len(keywords))
	for k := range keywords {
		keys = append(keys, k)
	}
	return keys
}
