package statestore

import (
	"errors"
	"testing"
)

func TestParseHistoryExpr(t *testing.T) {
	for _, s := range []string{"full", "none"} {
		got, err := ParseHistoryExpr(s)
		if err != nil {
			t.Errorf("ParseHistoryExpr(%q): %v", s, err)
		}
		if string(got) != s {
			t.Errorf("ParseHistoryExpr(%q) = %q", s, got)
		}
	}
	for _, s := range []string{"", "hash", "Full"} {
		if _, err := ParseHistoryExpr(s); !errors.Is(err, ErrUnknownHistoryExpr) {
			t.Errorf("ParseHistoryExpr(%q): want ErrUnknownHistoryExpr, got %v", s, err)
		}
	}
}
