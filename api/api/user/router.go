package user

import (
	"github.com/gin-gonic/gin"
	"log"
	"test.com/api/api/midd"
	"test.com/api/api/midd/ratelimit"
	"test.com/api/api/rpc"
	"test.com/api/pkg/ws"
	"test.com/api/router"
	common "test.com/others"
	"test.com/others/encrypts"
)

type RouterUser struct {
}

func init() {
	log.Println("init user router")
	ru := &RouterUser{}
	router.Register(ru)
}

func (*RouterUser) Route(r *gin.Engine) {
	rpc.InitRpcUserClient()

	h := NewHandler()
	r.POST("/refresh", h.refreshToken)
	r.POST("/iteration/login/getCaptcha", h.getCaptcha)
	r.POST("/iteration/login/register", h.register)
	r.POST("/iteration/login", h.login)

	org := r.Group("/iteration/organization")
	org.Use(midd.JWT_Midd())
	org.Use(ratelimit.Middleware())
	org.POST("/_getOrgList", h.myOrgList)

	wsGroup := r.Group("/iteration/ws")
	wsGroup.GET("/notify", ws.Authenticator(midd.MemberIdFromTokenWithIP))
	online := r.Group("/iteration/ws")
	online.Use(midd.JWT_Midd())
	online.Use(ratelimit.Middleware())
	online.GET("/online", func(c *gin.Context) {
		result := &common.Result{}
		hub := ws.Default()
		members := hub.OnlineMembers()
		ids := make([]string, 0, len(members))
		for _, id := range members {
			ids = append(ids, encrypts.EncryptNoErr(id))
		}
		c.JSON(200, result.Success(gin.H{
			"total":   hub.TotalOnline(),
			"members": ids,
		}))
	})
}
