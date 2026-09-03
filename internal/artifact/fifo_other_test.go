//go:build !unix

package artifact

import "errors"

func makeFIFO(string) error { return errors.New("unsupported") }
