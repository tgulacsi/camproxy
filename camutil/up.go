/*
Copyright 2013 Tamás Gulácsi

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package camutil

import (
	"log"
	"net/http"
	"os"

	"camlistore.org/pkg/client"
	"camlistore.org/pkg/syncutil"
)

// copied from camlistore.org/cmd/camput/uploader.go
type Uploader struct {
	*client.Client

	// fdGate guards gates the creation of file descriptors.
	fdGate *syncutil.Gate

	pwd string
	//statCache UploadCache
	//haveCache HaveCache

	fs http.FileSystem // virtual filesystem to read from; nil means OS filesystem.
}

// copied from camlistore.org/cmd/camput/camput.go
func NewUploader(server string) (*Uploader, error) {
	cc, err := NewClient(server)
	if err != nil {
		return nil, err
	}
	if !Verbose {
		cc.SetLogger(nil)
	}

	tr := cc.TransportForConfig(
		&client.TransportConfig{
			Proxy:   http.ProxyFromEnvironment,
			Verbose: Verbose,
		})
	cc.SetHTTPClient(&http.Client{Transport: tr})

	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("os.Getwd: %v", err)
	}

	return &Uploader{
		Client: cc,
		pwd:    pwd,
		fdGate: syncutil.NewGate(100), // gate things that waste fds, assuming a low system limit
	}, nil
}
