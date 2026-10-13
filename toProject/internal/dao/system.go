package dao

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"

	"google.golang.org/grpc/metadata"
)

type SystemDao struct {
	conn *gorms.GormConn
}

func NewSystemDao() *SystemDao {
	return &SystemDao{
		conn: gorms.New(),
	}
}

type ESResponse struct {
	Hits struct {
		Hits []struct {
			Source data.SystemLog `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

func memberIdFromMetadata(ctx context.Context) int64 {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0
	}
	for _, v := range md.Get("x-member-id") {
		if parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil && parsed > 0 {
			return parsed
		}
	}
	return 0
}

func (d *SystemDao) List(ctx context.Context) ([]*data.SystemNotify, error) {
	var list []*data.SystemNotify
	memberId := memberIdFromMetadata(ctx)
	if memberId <= 0 {
		return list, nil
	}
	err := d.conn.Reader(ctx).WithContext(ctx).
		Raw(`SELECT id, msg_key AS title, payload AS content, type,
			DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at
			FROM notification
			WHERE JSON_VALID(payload) AND JSON_EXTRACT(payload, '$.to_member_id') = ?
			ORDER BY id DESC
			LIMIT 200`, memberId).
		Scan(&list).Error
	return list, err
}

func (d *SystemDao) Query(ctx context.Context, traceId, level string, elkAddr, elkIndex string) ([]*data.SystemLog, error) {
	url := elkAddr + "/" + elkIndex + "/_search"

	reqBody := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"must": []map[string]any{},
			},
		},
		"sort": []map[string]any{
			{"@timestamp": map[string]any{"order": "desc"}},
		},
		"size": 100,
	}

	must := reqBody["query"].(map[string]any)["bool"].(map[string]any)["must"].([]map[string]any)
	if traceId != "" {
		must = append(must, map[string]any{"match": map[string]any{"traceId": traceId}})
	}
	if level != "" {
		must = append(must, map[string]any{"match": map[string]any{"level": level}})
	}
	reqBody["query"].(map[string]any)["bool"].(map[string]any)["must"] = must

	bs, _ := json.Marshal(reqBody)

	req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(bs))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var esResp ESResponse
	err = json.NewDecoder(resp.Body).Decode(&esResp)
	if err != nil {
		return nil, err
	}

	var logs []*data.SystemLog
	for _, hit := range esResp.Hits.Hits {
		logs = append(logs, &hit.Source)
	}

	return logs, nil
}

func (d *SystemDao) Clear(ctx context.Context) error {
	var cursor uint64
	for {
		keys, next, err := Rc.Scan(ctx, cursor, "*", 500)
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			Rc.Delete(ctx, keys)
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}
