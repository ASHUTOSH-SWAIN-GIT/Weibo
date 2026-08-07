package backend

func orString(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
