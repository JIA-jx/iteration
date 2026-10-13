package ws

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"golang.org/x/net/websocket"
)

const (
	writeTimeout        = 5 * time.Second
	readTimeout         = 90 * time.Second
	sendBufferSize      = 64
	maxClientsPerMember = 5
	maxTotalClients     = 10000
)

type Message struct {
	Type   string `json:"type"`
	Title  string `json:"title,omitempty"`
	Body   string `json:"body,omitempty"`
	BizId  int64  `json:"biz_id,omitempty"`
	Extra  string `json:"extra,omitempty"`
	SentAt int64  `json:"sent_at"`
}

type client struct {
	memberId int64
	conn     *websocket.Conn
	send     chan Message
	closeOne sync.Once
}

func (c *client) close() {
	c.closeOne.Do(func() {
		close(c.send)
		_ = c.conn.Close()
	})
}

type Hub struct {
	mu      sync.RWMutex
	clients map[int64]map[*client]struct{}
}

var (
	defaultHub = NewHub()
	hubOnce    sync.Once
)

func Default() *Hub {
	hubOnce.Do(func() {
		if defaultHub == nil {
			defaultHub = NewHub()
		}
	})
	return defaultHub
}

func NewHub() *Hub {
	return &Hub{clients: make(map[int64]map[*client]struct{})}
}

func Authenticator(memberIdFromToken func(c *gin.Context, token string) (int64, error)) gin.HandlerFunc {
	hub := Default()

	return func(c *gin.Context) {
		token := c.GetHeader("Authorization")
		token = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(token), "Bearer "))
		if token == "" {
			token = tokenFromSubProtocol(c.GetHeader("Sec-WebSocket-Protocol"))
		}
		if token == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "缺少 token"})
			return
		}

		memberId, err := memberIdFromToken(c, token)
		if err != nil || memberId <= 0 {
			zap.L().Warn("ws auth failed", zap.Error(err))
			c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "无效 token"})
			return
		}

		handler := websocket.Handler(func(conn *websocket.Conn) {
			hub.serve(memberId, conn)
		})
		handler.ServeHTTP(c.Writer, c.Request)
	}
}

func (h *Hub) serve(memberId int64, conn *websocket.Conn) {
	conn.MaxPayloadBytes = 1 << 20

	cl := &client{
		memberId: memberId,
		conn:     conn,
		send:     make(chan Message, sendBufferSize),
	}
	if !h.canRegister(memberId) {
		zap.L().Warn("ws connection limit reached", zap.Int64("member_id", memberId))
		_ = conn.Close()
		return
	}
	h.register(cl)
	defer h.unregister(cl)

	zap.L().Info("ws connected", zap.Int64("member_id", memberId), zap.Int("online", h.OnlineCount(memberId)))

	go h.writeLoop(cl)
	h.readLoop(cl)
}

func (h *Hub) canRegister(memberId int64) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.clients) >= maxTotalClients {
		return false
	}
	return len(h.clients[memberId]) < maxClientsPerMember
}

func (h *Hub) register(cl *client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.clients[cl.memberId] == nil {
		h.clients[cl.memberId] = make(map[*client]struct{})
	}
	h.clients[cl.memberId][cl] = struct{}{}
}

func (h *Hub) unregister(cl *client) {
	h.mu.Lock()
	if set, ok := h.clients[cl.memberId]; ok {
		delete(set, cl)
		if len(set) == 0 {
			delete(h.clients, cl.memberId)
		}
	}
	h.mu.Unlock()

	cl.close()
}

func (h *Hub) readLoop(cl *client) {
	_ = cl.conn.SetReadDeadline(time.Now().Add(readTimeout))
	for {
		var raw string
		if err := websocket.Message.Receive(cl.conn, &raw); err != nil {
			return
		}
		_ = cl.conn.SetReadDeadline(time.Now().Add(readTimeout))

		if raw == "ping" || raw == "pong" {
			h.trySend(cl, Message{Type: "pong", SentAt: time.Now().UnixMilli()})
		}
	}
}

func (h *Hub) writeLoop(cl *client) {
	defer func() {
		if r := recover(); r != nil {
			zap.L().Error("ws write loop panic", zap.Any("r", r))
		}
	}()

	for msg := range cl.send {
		if err := cl.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
			return
		}
		if err := websocket.JSON.Send(cl.conn, msg); err != nil {
			return
		}
	}
}

func (h *Hub) trySend(cl *client, msg Message) {
	defer func() {
		_ = recover()
	}()

	select {
	case cl.send <- msg:
	default:
		zap.L().Warn("ws send buffer full, drop message", zap.Int64("member_id", cl.memberId))
	}
}

func (h *Hub) SendToMember(memberId int64, msg Message) int {
	if memberId <= 0 {
		return 0
	}
	if msg.SentAt == 0 {
		msg.SentAt = time.Now().UnixMilli()
	}

	h.mu.RLock()
	set := h.clients[memberId]
	targets := make([]*client, 0, len(set))
	for cl := range set {
		targets = append(targets, cl)
	}
	h.mu.RUnlock()

	for _, cl := range targets {
		h.trySend(cl, msg)
	}
	return len(targets)
}

func (h *Hub) OnlineCount(memberId int64) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients[memberId])
}

func (h *Hub) TotalOnline() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	total := 0
	for _, set := range h.clients {
		total += len(set)
	}
	return total
}

func (h *Hub) OnlineMembers() []int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	members := make([]int64, 0, len(h.clients))
	for id := range h.clients {
		members = append(members, id)
	}
	return members
}

type NotifyPayload struct {
	TaskId     string `json:"task_id"`
	Type       string `json:"type"`
	BizId      int64  `json:"biz_id"`
	ToMemberId int64  `json:"to_member_id"`
	Title      string `json:"title"`
	Content    string `json:"content"`
	Ts         int64  `json:"ts"`
}

func (h *Hub) PushNotify(raw []byte) (int, error) {
	var payload NotifyPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, err
	}

	msg := Message{
		Type:   payload.Type,
		Title:  payload.Title,
		Body:   payload.Content,
		BizId:  payload.BizId,
		SentAt: payload.Ts,
	}
	if msg.SentAt > 0 && msg.SentAt < 1e12 {
		msg.SentAt = msg.SentAt * 1000
	}
	if msg.SentAt == 0 {
		msg.SentAt = time.Now().UnixMilli()
	}
	if payload.TaskId != "" {
		msg.Extra = "task_id=" + payload.TaskId
	}

	return h.SendToMember(payload.ToMemberId, msg), nil
}

func ParseMemberId(raw string) (int64, error) {
	return strconv.ParseInt(raw, 10, 64)
}

func tokenFromSubProtocol(raw string) string {
	for _, part := range strings.Split(raw, ",") {
		v := strings.TrimSpace(part)
		if v == "" || strings.EqualFold(v, "bearer") {
			continue
		}
		return strings.TrimSpace(strings.TrimPrefix(v, "Bearer "))
	}
	return ""
}
