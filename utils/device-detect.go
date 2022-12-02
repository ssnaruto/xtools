package utils

import (
	"regexp"
	"strings"
)

type DeviceDetect struct {
	matchMobile *regexp.Regexp

	matchIpad *regexp.Regexp

	matchTablet *regexp.Regexp
	matchRX34   *regexp.Regexp

	matchFolio *regexp.Regexp

	matchLinux   *regexp.Regexp
	matchAndroid *regexp.Regexp
	matchFennec  *regexp.Regexp

	matchMacOs *regexp.Regexp
	matchSilk  *regexp.Regexp

	matchGTP10 *regexp.Regexp

	matchMB511 *regexp.Regexp
	matchRUTEM *regexp.Regexp
}

func (r *DeviceDetect) GetDeviceByUserAgent(userAgent string) string {

	if r.matchIpad.MatchString(userAgent) ||
		(r.matchTablet.MatchString(userAgent) && !r.matchRX34.MatchString(userAgent)) ||
		r.matchFolio.MatchString(userAgent) ||
		(r.matchLinux.MatchString(userAgent) && r.matchAndroid.MatchString(userAgent) && !r.matchFennec.MatchString(userAgent)) ||
		(r.matchMacOs.MatchString(userAgent) && r.matchSilk.MatchString(userAgent)) ||
		r.matchGTP10.MatchString(userAgent) ||
		(r.matchMB511.MatchString(userAgent) && r.matchRUTEM.MatchString(userAgent)) {

		return "tablet"

	} else if r.matchMobile.MatchString(userAgent) {

		return "mobile"

	}

	return "desktop"
}

func NewDeviceDetect() DeviceDetect {
	return DeviceDetect{
		matchMobile:  regexp.MustCompile(strings.ToLower(`Mobile|iP(hone|od)|Android|BlackBerry|IEMobile|Kindle|Silk-Accelerated|(hpw|web)OS|Opera M(obi|ini)`)),
		matchIpad:    regexp.MustCompile(strings.ToLower(`iPad`)),
		matchTablet:  regexp.MustCompile(strings.ToLower(`tablet`)),
		matchRX34:    regexp.MustCompile(strings.ToLower(`RX-34`)),
		matchFolio:   regexp.MustCompile(strings.ToLower(`FOLIO`)),
		matchLinux:   regexp.MustCompile(strings.ToLower(`Linux`)),
		matchAndroid: regexp.MustCompile(strings.ToLower(`Android`)),
		matchFennec:  regexp.MustCompile(strings.ToLower(`Fennec|mobi|HTC.Magic|HTCX06HT|Nexus.One|SC-02B|fone.945`)),
		matchMacOs:   regexp.MustCompile(strings.ToLower(`Mac.OS`)),
		matchSilk:    regexp.MustCompile(strings.ToLower(`Silk`)),
		matchGTP10:   regexp.MustCompile(strings.ToLower(`GT-P10|SC-01C|SHW-M180S|SGH-T849|SCH-I800|SHW-M180L|SPH-P100|SGH-I987|zt180|HTC(.Flyer|\_Flyer)|Sprint.ATP51|ViewPad7|pandigital(sprnova|nova)|Ideos.S7|Dell.Streak.7|Advent.Vega|A101IT|A70BHT|MID7015|Next2|nook`)),
		matchMB511:   regexp.MustCompile(strings.ToLower(`MB511`)),
		matchRUTEM:   regexp.MustCompile(strings.ToLower(`RUTEM`)),
	}
}
