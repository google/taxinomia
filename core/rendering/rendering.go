/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package rendering is a forwarding package kept for import-path
// compatibility. The package moved to web/rendering. Every name here is an
// alias for the same type, so existing code keeps compiling and interoperates
// with code using the new path.
//
// Deprecated: import github.com/google/taxinomia/web/rendering instead. This
// forwarding package will be removed in a future cleanup release.
package rendering

import (
	webrendering "github.com/google/taxinomia/web/rendering"
)

// TableRenderer is an alias for rendering.TableRenderer (web/rendering).
type TableRenderer = webrendering.TableRenderer

// NewTableRenderer forwards to rendering.NewTableRenderer (web/rendering).
var NewTableRenderer = webrendering.NewTableRenderer
