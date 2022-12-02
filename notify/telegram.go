package notify

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/ssnaruto/xtools/utils"
)

func NewTelegram(chatId, apiURL string) NotifyChannel {
	return &Telegram{
		ChatId: chatId,
		ApiUrl: apiURL,
	}
}

type Telegram struct {
	ChatId string
	ApiUrl string
}

func (t *Telegram) PushMessage(format string, a ...interface{}) error {

	form := url.Values{}
	form.Add("chat_id", t.ChatId)
	form.Add("parse_mode", "HTML")
	if format != "" {
		form.Add("text", fmt.Sprintf(format, a...))
	} else {
		form.Add("text", fmt.Sprintln(a...))
	}

	resp, err := utils.ReqPost(
		t.ApiUrl,
		5,
		[]byte(form.Encode()),
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
	)

	if err != nil {
		return err
	}

	var response teleApiResponse
	err = json.Unmarshal(resp, &response)
	if err != nil {
		return fmt.Errorf("Parse telegram api response fail: %s", err)
	}

	if response.Ok == false {
		return fmt.Errorf("Send message fail")
	}

	return nil
}

type teleApiResponse struct {
	Ok bool
}
