package compose

// CAMountPath is where the per-check CA certificate is bind-mounted, read-only, into every
// container created from the service under test.
const CAMountPath = "/etc/stutter/ca.pem"

// CAVariables are the variables that point a runtime at an extra CA file. Each is set to
// CAMountPath on every container created from the service under test, over any compose or image
// value.
func CAVariables() [4]string {
	return [4]string{"SSL_CERT_FILE", "NODE_EXTRA_CA_CERTS", "REQUESTS_CA_BUNDLE", "AWS_CA_BUNDLE"}
}

// CAEnvironment maps each of CAVariables to CAMountPath: the environment a target container's spec
// is given. A fresh map per call.
func CAEnvironment() map[string]string {
	names := CAVariables()
	out := make(map[string]string, len(names))

	for _, name := range names {
		out[name] = CAMountPath
	}

	return out
}
