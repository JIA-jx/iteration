package event

type UserRegisterEvent struct {
	Uid    int64  `json:"uid"`
	Mobile string `json:"mobile"`
	Email  string `json:"email"`
	Name   string `json:"name"`
}

func (UserRegisterEvent) Topic() string { return "user_register" }
