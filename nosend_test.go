package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// sendingCalls maps each method that sends user-visible traffic to the only
// functions allowed to call it. Messages go out only through the shared send
// path (deliver), read receipts only through mark_read, and presence and chat
// state only through the API's presence and typing methods (typing and online
// status stay off unless a client asks), plus the withdrawal of a presence a
// client set once no client is left. liveClient's
// methods are the seam's one call into whatsmeow for each. Methods mapped to
// nothing must not be called at all.
var sendingCalls = map[string][]string{
	"SendMessage":                    {"liveClient.SendMessage", "handler.deliver"},
	"MarkRead":                       {"liveClient.MarkRead", "handler.markRead"},
	"SendPresence":                   {"liveClient.SendPresence", "handler.apiPresence", "handler.withdrawPresence"},
	"SendChatPresence":               {"liveClient.SendChatPresence", "handler.apiTyping"},
	"SendPeerMessage":                nil,
	"RevokeMessage":                  nil,
	"BuildReaction":                  nil,
	"SetForceActiveDeliveryReceipts": nil,
	"SetStatusMessage":               nil,
	"SendAppState":                   nil,
	"SendMediaRetryReceipt":          nil,
	"SubscribePresence":              nil,
	"SetPrivacySetting":              nil,
}

// TestNoUnaskedSending is a static check of the rule above over the
// daemon's source; TestNothingSendsPresenceUnasked checks it at run time.
func TestNoUnaskedSending(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			caller := fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				typ := fn.Recv.List[0].Type
				if star, ok := typ.(*ast.StarExpr); ok {
					typ = star.X
				}
				if id, ok := typ.(*ast.Ident); ok {
					caller = id.Name + "." + caller
				}
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				allowed, watched := sendingCalls[sel.Sel.Name]
				if !watched {
					return true
				}
				for _, a := range allowed {
					if a == caller {
						return true
					}
				}
				t.Errorf("%s: %s calls %s", fset.Position(call.Pos()), caller, sel.Sel.Name)
				return true
			})
		}
	}
}
