package realip

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/ssnaruto/xtools/logx"
	"github.com/ssnaruto/xtools/utils"

	"github.com/gofiber/fiber/v2"
	"github.com/robfig/cron/v3"
)

const (
	CLOUDFLARE_IP_SOURCE = "https://api.cloudflare.com/client/v4/ips"
)

var cidrs []*net.IPNet
var mux sync.Mutex
var localCidrs = []string{
	"127.0.0.1/8",    // localhost
	"10.0.0.0/8",     // 24-bit block
	"172.16.0.0/12",  // 20-bit block
	"192.168.0.0/16", // 16-bit block
	"169.254.0.0/16", // link local address
	"::1/128",        // localhost IPv6
	"fc00::/7",       // unique local address IPv6
	"fe80::/10",      // link local address IPv6,
}

func Init(ctx context.Context) error {
	cfIPList, err := getCloudFlareIpRanges()
	if err != nil {
		return err
	}

	updatePrivateIPList(utils.ArrayStringMerge(localCidrs, cfIPList))

	go func() {
		c := cron.New()
		c.AddFunc("@daily", func() {
			newCfIPList, _ := getCloudFlareIpRanges()
			for _, ipRange := range newCfIPList {
				if !utils.InArrayString(ipRange, cfIPList) {
					updatePrivateIPList(utils.ArrayStringMerge(localCidrs, newCfIPList))
					cfIPList = newCfIPList
					return
				}
			}
		})
		c.Start()

		select {
		case <-ctx.Done():
			c.Stop()
		}
	}()

	logx.Info("RealIP init success...")
	return nil
}

func updatePrivateIPList(cidrsList []string) {
	mux.Lock()
	cidrs = make([]*net.IPNet, len(cidrsList))
	for i, maxCidrBlock := range cidrsList {
		_, cidr, _ := net.ParseCIDR(maxCidrBlock)
		cidrs[i] = cidr
	}
	mux.Unlock()

	logx.Info("Update private ip success....")
}

// isLocalAddress works by checking if the address is under private CIDR blocks.
// List of private CIDR blocks can be seen on :
//
// https://en.wikipedia.org/wiki/Private_network
//
// https://en.wikipedia.org/wiki/Link-local_address
func isPrivateAddress(address string) (bool, error) {
	ipAddress := net.ParseIP(address)
	if ipAddress == nil {
		return false, errors.New("address is not valid")
	}

	for i := range cidrs {
		if cidrs[i].Contains(ipAddress) {
			return true, nil
		}
	}

	return false, nil
}

func GetIPFromHttpRequest(r *http.Request) string {
	// Fetch header value
	xRealIP := r.Header.Get("X-Real-Ip")
	xForwardedFor := r.Header.Get("X-Forwarded-For")
	CfForwardedFor := r.Header.Get("CF-Connecting-IP")
	if CfForwardedFor != "" {
		xForwardedFor = CfForwardedFor
	}

	// If both empty, return IP from remote address
	if xRealIP == "" && xForwardedFor == "" {
		var remoteIP string

		// If there are colon in remote address, remove the port number
		// otherwise, return remote address as is
		if strings.ContainsRune(r.RemoteAddr, ':') {
			remoteIP, _, _ = net.SplitHostPort(r.RemoteAddr)
		} else {
			remoteIP = r.RemoteAddr
		}

		return remoteIP
	}

	// Check list of IP in X-Forwarded-For and return the first global address
	for _, address := range strings.Split(xForwardedFor, ",") {
		address = strings.TrimSpace(address)
		isPrivate, err := isPrivateAddress(address)
		if !isPrivate && err == nil {
			return address
		}
	}

	// If nothing succeed, return X-Real-IP
	return xRealIP
}

// FromRequest return client's real public IP address from http request headers.
func GetIPFromFastHttpRequest(ctx *fiber.Ctx) string {
	// Fetch header value
	xRealIP := ctx.Get("X-Real-Ip")
	xForwardedFor := ctx.Get("X-Forwarded-For")
	CfForwardedFor := ctx.Get("CF-Connecting-IP")

	devMod := ctx.Query("dev")
	if devMod == "1" {
		fmt.Fprintf(ctx, "%s\n%s\n%s\n", xRealIP, xForwardedFor, xForwardedFor)
	}
	if CfForwardedFor != "" {
		xForwardedFor = CfForwardedFor
	}

	// If both empty, return IP from remote address
	if xRealIP == "" && xForwardedFor == "" {
		return ctx.IP()
	}

	// Check list of IP in X-Forwarded-For and return the first global address
	for _, address := range strings.Split(xForwardedFor, ",") {
		address = strings.TrimSpace(address)
		isPrivate, err := isPrivateAddress(address)
		if !isPrivate && err == nil {
			return address
		}
	}

	// If nothing succeed, return X-Real-IP
	return xRealIP
}

func getCloudFlareIpRanges() ([]string, error) {

	var resp []byte
	var err error

	for i := 0; i < 5; i++ {
		resp, err = utils.ReqGet(CLOUDFLARE_IP_SOURCE, 5, map[string]string{})
		if err == nil {
			break
		}
	}

	var cfResponse CFIP
	err = json.Unmarshal(resp, &cfResponse)
	if err != nil || cfResponse.Success != true {
		return []string{}, err
	}

	return utils.ArrayStringMerge(cfResponse.Result.IPV4, cfResponse.Result.IPV6), nil
}

type CFIP struct {
	Result struct {
		IPV4 []string `json:"ipv4_cidrs"`
		IPV6 []string `json:"ipv6_cidrs"`
	} `json:"result"`
	Success bool `json:"success"`
}
