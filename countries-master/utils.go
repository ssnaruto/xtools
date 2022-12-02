package countries

func GetCountryName(countryName string) string {
	switch countryName {
	case "Vietnam":
		return "Việt Nam"
	}

	return countryName
}
