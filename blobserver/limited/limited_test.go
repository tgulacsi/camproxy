// Copyright 2020 The Perkeep Authors
//
// SPDX-License-Identifier: Apache-2.0

/*
Copyright 2020 The Perkeep Authors

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

package limited

import (
	"testing"

	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/blobserver/memory"
	"perkeep.org/pkg/blobserver/storagetest"
)

func TestStorage(t *testing.T) {
	storagetest.Test(t, func(t *testing.T) blobserver.Storage {
		sto := memory.NewCache(16 << 20)
		return NewStorage(sto, 100)
	})
}
