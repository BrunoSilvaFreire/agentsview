package parser

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// KandevDBName is the filename of Kandev's application database.
const KandevDBName = "kandev.db"

// Kandev sessions live in the task_sessions / task_session_messages tables of
// the Kandev application database at <root>/data/kandev.db. The database also
// holds workspaces, auth tokens, and plugin state, so this provider opens it
// read-only and selects named columns only.
type kandevProviderFactory struct {
	def AgentDef
}

func newKandevProviderFactory(def AgentDef) ProviderFactory {
	return kandevProviderFactory{def: cloneAgentDef(def)}
}

func (f kandevProviderFactory) Definition() AgentDef {
	return cloneAgentDef(f.def)
}

func (f kandevProviderFactory) Capabilities() Capabilities {
	return withDBBackedRawCapture(kandevProviderCapabilities())
}

func (f kandevProviderFactory) NewProvider(cfg ProviderConfig) Provider {
	cfg = cfg.Clone()
	cfg.Roots = normalizeKandevRoots(cfg.Roots, cfg.StableSourceSnapshots)
	spec := kandevProviderSpec(cfg.StableSourceSnapshots)
	return &dbBackedProvider{
		Def:     cloneAgentDef(f.def),
		Caps:    withDBBackedRawCapture(spec.caps),
		Config:  cfg,
		spec:    spec,
		sources: newDBBackedSourceSet(spec, cfg.Roots),
	}
}

func kandevProviderCapabilities() Capabilities {
	return Capabilities{
		Source: dbBackedSourceCapabilities(CapabilityNotApplicable),
		Content: ContentCapabilities{
			FirstMessage: CapabilitySupported,
			SessionName:  CapabilitySupported,
			Cwd:          CapabilitySupported,
			Thinking:     CapabilitySupported,
			ToolCalls:    CapabilitySupported,
			ToolResults:  CapabilitySupported,
			Model:        CapabilitySupported,
		},
	}
}

func kandevProviderSpec(stableSnapshot bool) dbBackedProviderSpec {
	return dbBackedProviderSpec{
		agent:  AgentKandev,
		dbName: KandevDBName,
		findDB: kandevDBPath,
		streamMeta: func(
			ctx context.Context,
			dbPath string,
			yield func(dbBackedSessionMeta) error,
		) error {
			return forEachKandevSessionMeta(ctx, dbPath, stableSnapshot, yield)
		},
		metaForID: func(
			ctx context.Context, dbPath, sessionID string,
		) (dbBackedSessionMeta, bool, error) {
			return kandevSessionMeta(ctx, dbPath, sessionID, stableSnapshot)
		},
		parse: func(
			ctx context.Context, dbPath, sessionID, machine string,
		) ([]ParseResult, error) {
			result, err := parseKandevSession(ctx, dbPath, sessionID, machine, stableSnapshot)
			if err != nil || result == nil {
				return nil, err
			}
			return []ParseResult{*result}, nil
		},
		caps: kandevProviderCapabilities(),
	}
}

// normalizeKandevRoots maps configured roots to the directory that holds
// kandev.db. A configured root is usually ~/.kandev, whose database lives one
// level down in data/. A materialized raw snapshot places kandev.db directly
// inside the snapshot root, which only the stable-snapshot mode accepts.
func normalizeKandevRoots(roots []string, directRoot bool) []string {
	cleaned := cleanJSONLRoots(roots)
	out := make([]string, 0, len(cleaned))
	seen := make(map[string]struct{}, len(cleaned))
	for _, root := range cleaned {
		normalized := normalizeKandevRoot(root, directRoot)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func normalizeKandevRoot(root string, directRoot bool) string {
	root = filepath.Clean(root)
	if root == "" || root == "." {
		return ""
	}
	if directRoot && IsRegularFile(filepath.Join(root, KandevDBName)) {
		return root
	}
	if filepath.Base(root) == "data" {
		return root
	}
	return filepath.Join(root, "data")
}

func kandevDBPath(dir string) string {
	if dir == "" {
		return ""
	}
	path := filepath.Join(dir, KandevDBName)
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return ""
	}
	return path
}

// KandevSQLiteVirtualPath identifies one session inside the shared database.
func KandevSQLiteVirtualPath(dbPath, sessionID string) string {
	return VirtualSourcePath(dbPath, sessionID)
}

func openKandevDB(dbPath string, stableSnapshot bool) (*sql.DB, error) {
	db, err := openSQLiteReadOnly(dbPath, sqliteReadOptions{
		stableSnapshot: stableSnapshot,
		busyTimeoutMS:  3000,
	})
	if err != nil {
		return nil, fmt.Errorf("opening kandev db %s: %w", dbPath, err)
	}
	return db, nil
}

// kandevTime parses the SQLite timestamp spellings Kandev writes, including
// the space-separated nanoseconds-plus-offset form, falling back to the shared
// transcript layouts.
func kandevTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		time.RFC3339Nano,
	} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC()
		}
	}
	return parseTimestamp(raw)
}

func kandevColumn(
	ctx context.Context, db *sql.DB, table, column string,
) (bool, error) {
	return zcodeTableHasColumn(ctx, db, table, column)
}

type kandevSessionRow struct {
	id               string
	taskID           sql.NullString
	name             sql.NullString
	workspacePath    sql.NullString
	baseBranch       sql.NullString
	startedAt        sql.NullString
	completedAt      sql.NullString
	updatedAt        sql.NullString
	profile          sql.NullString
	repository       sql.NullString
	tokensIn         sql.NullInt64
	tokensCachedIn   sql.NullInt64
	tokensOut        sql.NullInt64
	downstreamACP    sql.NullString
	isPassthrough    sql.NullInt64
	workflowStepID   sql.NullString
	metadata         sql.NullString
	hasTokensIn      bool
	hasTokensCached  bool
	hasTokensOut     bool
	hasDownstreamACP bool
	hasPassthrough   bool
	hasWorkflowStep  bool
}

func kandevSessionSelect(ctx context.Context, db *sql.DB) (string, error) {
	optional := []struct {
		column string
		expr   string
	}{
		{"name", "COALESCE(name, '')"},
		{"workspace_path", "COALESCE(workspace_path, '')"},
		{"base_branch", "COALESCE(base_branch, '')"},
		{"started_at", "COALESCE(started_at, '')"},
		{"completed_at", "COALESCE(completed_at, '')"},
		{"updated_at", "COALESCE(updated_at, '')"},
		{"agent_profile_snapshot", "COALESCE(agent_profile_snapshot, '')"},
		{"repository_snapshot", "COALESCE(repository_snapshot, '')"},
		{"tokens_in", "COALESCE(tokens_in, 0)"},
		{"tokens_cached_in", "COALESCE(tokens_cached_in, 0)"},
		{"tokens_out", "COALESCE(tokens_out, 0)"},
		{"downstream_acp_session_id", "COALESCE(downstream_acp_session_id, '')"},
		{"is_passthrough", "COALESCE(is_passthrough, 0)"},
		{"workflow_step_id_at_creation", "COALESCE(workflow_step_id_at_creation, '')"},
		{"metadata", "COALESCE(metadata, '{}')"},
	}
	columns := []string{"id", "COALESCE(task_id, '')"}
	for _, opt := range optional {
		has, err := kandevColumn(ctx, db, "task_sessions", opt.column)
		if err != nil {
			return "", err
		}
		if has {
			columns = append(columns, opt.expr)
		} else {
			columns = append(columns, "''")
		}
	}
	return strings.Join(columns, ", "), nil
}

func kandevSessionRowFromScanner(scan func(...any) error) (kandevSessionRow, error) {
	var row kandevSessionRow
	if err := scan(
		&row.id,
		&row.taskID,
		&row.name,
		&row.workspacePath,
		&row.baseBranch,
		&row.startedAt,
		&row.completedAt,
		&row.updatedAt,
		&row.profile,
		&row.repository,
		&row.tokensIn,
		&row.tokensCachedIn,
		&row.tokensOut,
		&row.downstreamACP,
		&row.isPassthrough,
		&row.workflowStepID,
		&row.metadata,
	); err != nil {
		return kandevSessionRow{}, err
	}
	return row, nil
}

func forEachKandevSessionMeta(
	ctx context.Context, dbPath string, stableSnapshot bool, yield func(dbBackedSessionMeta) error,
) error {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil
	}
	db, err := openKandevDB(dbPath, stableSnapshot)
	if err != nil {
		return err
	}
	defer db.Close()

	selectList, err := kandevSessionSelect(ctx, db)
	if err != nil {
		return fmt.Errorf("describing kandev sessions: %w", err)
	}
	rows, err := db.QueryContext(ctx, `
		SELECT `+selectList+`
		  FROM task_sessions
		 ORDER BY id
	`)
	if err != nil {
		return fmt.Errorf("listing kandev sessions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		row, err := kandevSessionRowFromScanner(rows.Scan)
		if err != nil {
			return fmt.Errorf("scanning kandev session meta: %w", err)
		}
		if row.id == "" {
			continue
		}
		observeStreamingDiscoveryBuffer(ctx, 1)
		mtime, err := kandevSessionMtime(ctx, db, dbPath, row)
		if err != nil {
			return err
		}
		if err := yield(dbBackedSessionMeta{
			SessionID:   row.id,
			VirtualPath: KandevSQLiteVirtualPath(dbPath, row.id),
			FileMtime:   mtime,
		}); err != nil {
			return err
		}
	}
	return rows.Err()
}

func kandevSessionMeta(
	ctx context.Context, dbPath, sessionID string, stableSnapshot bool,
) (dbBackedSessionMeta, bool, error) {
	db, err := openKandevDB(dbPath, stableSnapshot)
	if err != nil {
		return dbBackedSessionMeta{}, false, err
	}
	defer db.Close()

	selectList, err := kandevSessionSelect(ctx, db)
	if err != nil {
		return dbBackedSessionMeta{}, false, err
	}
	row, err := kandevSessionRowFromScanner(
		db.QueryRowContext(ctx,
			`SELECT `+selectList+` FROM task_sessions WHERE id = ?`, sessionID,
		).Scan,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return dbBackedSessionMeta{}, false, nil
	}
	if err != nil {
		return dbBackedSessionMeta{}, false, fmt.Errorf("loading kandev session %s: %w", sessionID, err)
	}
	if err := ctx.Err(); err != nil {
		return dbBackedSessionMeta{}, false, err
	}
	mtime, err := kandevSessionMtime(ctx, db, dbPath, row)
	if err != nil {
		return dbBackedSessionMeta{}, false, err
	}
	return dbBackedSessionMeta{
		SessionID:   row.id,
		VirtualPath: KandevSQLiteVirtualPath(dbPath, row.id),
		FileMtime:   mtime,
	}, true, nil
}

// kandevSessionMtime folds the session's own activity timestamps with the
// container's physical mtimes so a session re-parses when either moves.
func kandevSessionMtime(
	ctx context.Context, db *sql.DB, dbPath string, row kandevSessionRow,
) (int64, error) {
	var maxMtime int64
	for _, raw := range []string{row.updatedAt.String, row.completedAt.String, row.startedAt.String} {
		if ts := kandevTime(raw); !ts.IsZero() {
			maxMtime = max(maxMtime, ts.UnixNano())
		}
	}
	if has, err := kandevColumn(ctx, db, "task_session_messages", "updated_at"); err == nil && has {
		var newest sql.NullString
		if err := db.QueryRowContext(ctx, `
			SELECT MAX(updated_at) FROM task_session_messages WHERE task_session_id = ?
		`, row.id).Scan(&newest); err == nil {
			if ts := kandevTime(newest.String); !ts.IsZero() {
				maxMtime = max(maxMtime, ts.UnixNano())
			}
		} else if ctx.Err() != nil {
			return 0, ctx.Err()
		}
	} else if err != nil {
		return 0, err
	}
	// The -shm index is deliberately excluded: readers rewrite it, including
	// this provider's own connection, which would make every scan report the
	// whole container as changed.
	if physical, err := sqliteDBCompositeMtime(dbPath, sqliteDBJournalSuffixes); err == nil {
		maxMtime = max(maxMtime, physical)
	}
	return maxMtime, nil
}

func parseKandevSession(
	ctx context.Context, dbPath, sessionID, machine string, stableSnapshot bool,
) (*ParseResult, error) {
	db, err := openKandevDB(dbPath, stableSnapshot)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	selectList, err := kandevSessionSelect(ctx, db)
	if err != nil {
		return nil, err
	}
	row, err := kandevSessionRowFromScanner(
		db.QueryRowContext(ctx,
			`SELECT `+selectList+` FROM task_sessions WHERE id = ?`, sessionID,
		).Scan,
	)
	if err != nil {
		return nil, fmt.Errorf("loading kandev session %s: %w", sessionID, err)
	}

	result, err := buildKandevParseResult(ctx, dbPath, machine, row, db)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func buildKandevParseResult(
	ctx context.Context, dbPath, machine string, row kandevSessionRow, db *sql.DB,
) (ParseResult, error) {
	startedAt := kandevTime(row.startedAt.String)
	endedAt := kandevTime(row.completedAt.String)
	if endedAt.IsZero() {
		endedAt = kandevTime(row.updatedAt.String)
	}
	if startedAt.IsZero() {
		startedAt = endedAt
	}
	if endedAt.IsZero() {
		endedAt = startedAt
	}

	repository := gjson.Parse(row.repository.String)
	workspace := strings.TrimSpace(row.workspacePath.String)
	cwd := workspace
	if cwd == "" {
		cwd = strings.TrimSpace(repository.Get("local_path").Str)
	}
	project := ""
	if name := strings.TrimSpace(repository.Get("name").Str); name != "" {
		project = name
	} else {
		project = ExtractProjectFromCwdWithBranchContext(ctx, cwd, "")
	}
	if project == "" {
		project = "kandev"
	}

	title := strings.TrimSpace(row.name.String)
	taskTitle, taskDescription := kandevTaskSummary(ctx, db, row.taskID.String)
	if title == "" {
		title = taskTitle
	}

	parsedMessages, err := buildKandevMessages(ctx, db, row.id)
	if err != nil {
		return ParseResult{}, err
	}
	userMessageCount := 0
	for _, msg := range parsedMessages {
		if msg.Role == RoleUser {
			userMessageCount++
		}
	}

	firstMessage := title
	if firstMessage == "" {
		firstMessage = taskDescription
	}
	if firstMessage == "" {
		for _, msg := range parsedMessages {
			if msg.Role == RoleUser && !msg.IsSystem && strings.TrimSpace(msg.Content) != "" {
				firstMessage = msg.Content
				break
			}
		}
	}
	if firstMessage != "" {
		firstMessage = truncate(strings.Join(strings.Fields(firstMessage), " "), 300)
	}

	branch := strings.TrimSpace(row.baseBranch.String)

	sess := ParsedSession{
		ID:                 "kandev:" + row.id,
		Project:            project,
		Machine:            machine,
		Agent:              AgentKandev,
		Cwd:                cwd,
		GitBranch:          branch,
		GitRemote:          strings.TrimSpace(repository.Get("remote_url").Str),
		FirstMessage:       firstMessage,
		SessionName:        title,
		SessionNamePresent: title != "",
		StartedAt:          startedAt,
		EndedAt:            endedAt,
		MessageCount:       len(parsedMessages),
		UserMessageCount:   userMessageCount,
		File: FileInfo{
			Path: KandevSQLiteVirtualPath(dbPath, row.id),
		},
	}
	if info, err := os.Stat(dbPath); err == nil {
		sess.File.Size = info.Size()
	}
	mtime, err := kandevSessionMtime(ctx, db, dbPath, row)
	if err != nil {
		return ParseResult{}, err
	}
	sess.File.Mtime = mtime

	if row.tokensOut.Valid && row.tokensOut.Int64 > 0 {
		sess.TotalOutputTokens = int(row.tokensOut.Int64)
		sess.HasTotalOutputTokens = true
	}

	return ParseResult{
		Session:  sess,
		Messages: parsedMessages,
	}, nil
}

func kandevTaskSummary(ctx context.Context, db *sql.DB, taskID string) (string, string) {
	if taskID == "" {
		return "", ""
	}
	has, err := kandevColumn(ctx, db, "tasks", "title")
	if err != nil || !has {
		return "", ""
	}
	var title, description sql.NullString
	err = db.QueryRowContext(ctx, `
		SELECT COALESCE(title, ''), COALESCE(description, '')
		  FROM tasks
		 WHERE id = ?
	`, taskID).Scan(&title, &description)
	if err != nil {
		if ctx.Err() != nil {
			return "", ""
		}
		return "", ""
	}
	return strings.TrimSpace(title.String), strings.TrimSpace(description.String)
}

type kandevMessageRow struct {
	id         string
	turnID     sql.NullString
	authorType sql.NullString
	content    sql.NullString
	msgType    sql.NullString
	metadata   sql.NullString
	createdAt  sql.NullString
}

// buildKandevMessages converts the session's message rows into transcript
// messages. Kandev records tool calls, tool output, and thinking as their own
// rows; those fold into assistant messages and expose tool results to the
// index. Bookkeeping rows (status, todo, script boot logs) carry no
// conversation and are skipped.
func buildKandevMessages(
	ctx context.Context, db *sql.DB, sessionID string,
) ([]ParsedMessage, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id,
		       COALESCE(turn_id, ''),
		       COALESCE(author_type, ''),
		       COALESCE(content, ''),
		       COALESCE(type, 'message'),
		       COALESCE(metadata, '{}'),
		       COALESCE(created_at, '')
		  FROM task_session_messages
		 WHERE task_session_id = ?
		 ORDER BY COALESCE(created_at, ''), id
	`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("listing kandev messages for %s: %w", sessionID, err)
	}
	defer rows.Close()

	parsed := make([]ParsedMessage, 0, 64)
	// callOwner maps a Kandev tool_call_id to the message that owns the call so
	// a later tool_result row can attach to it.
	callOwner := make(map[string]int)
	pending := make([]kandevMessageRow, 0, 64)
	for rows.Next() {
		var row kandevMessageRow
		if err := rows.Scan(
			&row.id, &row.turnID, &row.authorType, &row.content,
			&row.msgType, &row.metadata, &row.createdAt,
		); err != nil {
			return nil, fmt.Errorf("scanning kandev message row: %w", err)
		}
		if row.id == "" {
			continue
		}
		pending = append(pending, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	for _, row := range pending {
		msg, owner, ok := kandevMessage(ctx, len(parsed), row)
		if !ok {
			continue
		}
		if owner != "" {
			callOwner[owner] = msg.Ordinal
		}
		parsed = append(parsed, msg)
	}
	return parsed, nil
}

// kandevMessage returns the message for one row, the tool_call_id it owns (if
// any), and whether the row belongs in the transcript at all.
func kandevMessage(_ context.Context, ordinal int, row kandevMessageRow) (ParsedMessage, string, bool) {
	msgType := strings.ToLower(strings.TrimSpace(row.msgType.String))
	switch msgType {
	case "status", "todo", "script_execution":
		return ParsedMessage{}, "", false
	}

	role := RoleAssistant
	switch strings.ToLower(strings.TrimSpace(row.authorType.String)) {
	case "user":
		role = RoleUser
	case "system":
		role = RoleSystem
	case "agent", "assistant", "":
		role = RoleAssistant
	}

	msg := ParsedMessage{
		Ordinal:   ordinal,
		Role:      role,
		Timestamp: kandevTime(row.createdAt.String),
		IsSystem:  role == RoleSystem,
	}
	metadata := gjson.Parse(row.metadata.String)
	model := strings.TrimSpace(metadata.Get("model").Str)
	if model != "" {
		msg.Model = model
	}

	switch {
	case msgType == "thinking" || msgType == "reasoning":
		thinking := strings.TrimSpace(metadata.Get("thinking").Str)
		if thinking == "" {
			thinking = strings.TrimSpace(row.content.String)
		}
		if thinking == "" {
			return ParsedMessage{}, "", false
		}
		msg.HasThinking = true
		msg.ThinkingText = thinking
		return msg, "", true
	case strings.HasPrefix(msgType, "tool"), strings.HasPrefix(msgType, "script"):
		return kandevToolMessage(msg, msgType, row, metadata)
	}

	text := row.content.String
	if strings.TrimSpace(text) == "" {
		return ParsedMessage{}, "", false
	}
	// Kandev injects its MCP briefing as a user-role message. Keep the text as
	// evidence but mark it system so it never stands in for a real user turn.
	if strings.HasPrefix(strings.TrimSpace(text), "<kandev-system>") {
		msg.IsSystem = true
		msg.Role = RoleSystem
	}
	msg.Content = text
	msg.ContentLength = len(text)
	return msg, "", true
}

func kandevToolMessage(
	msg ParsedMessage, msgType string, row kandevMessageRow, metadata gjson.Result,
) (ParsedMessage, string, bool) {
	normalized := metadata.Get("normalized")
	kind := strings.TrimSpace(normalized.Get("kind").Str)
	if kind == "" {
		kind = msgType
	}
	payload := normalized.Get(kind)

	name := kind
	category := NormalizeToolCategory(kind)
	if category == "" {
		category = NormalizeToolCategory(msgType)
	}

	toolID := strings.TrimSpace(metadata.Get("tool_call_id").Str)
	if toolID == "" {
		toolID = row.id
	}

	inputJSON := "{}"
	if payload.Exists() {
		trimmed := map[string]any{}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal([]byte(payload.Raw), &raw); err == nil {
			for key, value := range raw {
				switch key {
				case "output", "result", "stdout", "stderr":
					continue
				}
				var decoded any
				if err := json.Unmarshal(value, &decoded); err == nil {
					trimmed[key] = decoded
				}
			}
			if encoded, err := json.Marshal(trimmed); err == nil {
				inputJSON = string(encoded)
			}
		}
	}

	call := ParsedToolCall{
		ToolUseID: toolID,
		ToolName:  name,
		Category:  category,
		InputJSON: inputJSON,
	}
	if path := strings.TrimSpace(payload.Get("file_path").Str); path != "" {
		call.FilePath = path
	}

	msg.HasToolUse = true
	msg.ToolCalls = append(msg.ToolCalls, call)

	if output := kandevToolOutput(payload); output != "" {
		msg.ToolResults = append(msg.ToolResults, ParsedToolResult{
			ToolUseID:     toolID,
			Source:        "kandev",
			Status:        strings.TrimSpace(metadata.Get("status").Str),
			ContentLength: len(output),
			ContentRaw:    kandevRawContent(output),
		})
	}
	if text := strings.TrimSpace(row.content.String); text != "" {
		msg.Content = text
		msg.ContentLength = len(text)
	}
	return msg, toolID, true
}

func kandevToolOutput(payload gjson.Result) string {
	for _, path := range []string{"output", "result"} {
		node := payload.Get(path)
		if !node.Exists() {
			continue
		}
		if node.Type == gjson.String {
			return node.Str
		}
		return node.Raw
	}
	return ""
}

func kandevRawContent(text string) string {
	encoded, err := json.Marshal(text)
	if err != nil {
		return ""
	}
	return string(encoded)
}

// kandevIntOrZero is a defensive helper for row columns that may arrive as
// TEXT in older Kandev schemas.
func kandevIntOrZero(raw string) int64 {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0
	}
	return value
}
