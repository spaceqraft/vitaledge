package parser

import "strings"

type statementSegment struct {
	text        string
	startOffset int
	index       int
}

func splitStatements(input string) []statementSegment {
	segments := make([]statementSegment, 0)
	start := 0
	segmentIndex := 1

	inSingle := false
	inDouble := false
	inBacktick := false
	inLineComment := false
	inBlockComment := false
	escaped := false

	for i := 0; i < len(input); i++ {
		ch := input[i]

		if inLineComment {
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}

		if inBlockComment {
			if ch == '*' && i+1 < len(input) && input[i+1] == '/' {
				inBlockComment = false
				i++
			}
			continue
		}

		if inSingle {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '\'' {
				inSingle = false
			}
			continue
		}

		if inDouble {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inDouble = false
			}
			continue
		}

		if inBacktick {
			if ch == '`' {
				if i+1 < len(input) && input[i+1] == '`' {
					i++
					continue
				}
				inBacktick = false
			}
			continue
		}

		if ch == '/' && i+1 < len(input) {
			next := input[i+1]
			if next == '/' {
				inLineComment = true
				i++
				continue
			}
			if next == '*' {
				inBlockComment = true
				i++
				continue
			}
		}

		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case ';':
			chunk := input[start:i]
			if strings.TrimSpace(chunk) != "" {
				segments = append(segments, statementSegment{text: chunk, startOffset: start, index: segmentIndex})
				segmentIndex++
			}
			start = i + 1
		}
	}

	chunk := input[start:]
	if strings.TrimSpace(chunk) != "" {
		segments = append(segments, statementSegment{text: chunk, startOffset: start, index: segmentIndex})
	}

	return segments
}

func lineColAtOffset(input string, offset int) (line int, col int) {
	line = 1
	col = 1

	if offset <= 0 {
		return line, col
	}
	if offset > len(input) {
		offset = len(input)
	}

	for i := 0; i < offset; i++ {
		if input[i] == '\n' {
			line++
			col = 1
			continue
		}
		col++
	}

	return line, col
}

func localToGlobal(seg statementSegment, full string, localLine int, localColumnZeroBased int) (line int, col int) {
	baseLine, baseCol := lineColAtOffset(full, seg.startOffset)
	if localLine <= 1 {
		return baseLine, baseCol + localColumnZeroBased
	}
	return baseLine + localLine - 1, localColumnZeroBased + 1
}
