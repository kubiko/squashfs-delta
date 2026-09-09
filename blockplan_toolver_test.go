// -*- Mode: Go; indent-tabs-mode: t -*-

/*
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 3 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// versionComp answers ToolVersion and nothing else: SEC_TOOLVER is the only
// thing under test here, and it asks a compressor for exactly that.
type versionComp struct {
	Compressor
	version string
}

func (v versionComp) ToolVersion() string { return v.version }

// fakeHpatchz writes a stand-in that answers -v with version, so a test can
// pick the drift instead of depending on what this machine has installed.
func fakeHpatchz(t *testing.T, version string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hpatchz")
	script := "#!/bin/sh\necho '" + version + "'\n"
	if err := os.WriteFile(path, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestCheckToolVersionsReportsDrift covers the whole point of the section: a
// tool that has moved since the delta was built is named, and the recorded
// version and the local one are both shown.
func TestCheckToolVersionsReportsDrift(t *testing.T) {
	recorded := []byte("hdiffz: HDiffPatch::hdiffz v1.0.0\n" +
		"hpatchz: HDiffPatch::hpatchz v1.0.0\n" +
		"xz: xz (XZ Utils) 5.0.0\n")

	var out strings.Builder
	comp := versionComp{version: "xz: xz (XZ Utils) 9.9.9"}
	checkToolVersions(context.Background(), &out, recorded, comp, fakeHpatchz(t, "HDiffPatch::hpatchz v9.9.9"))
	got := out.String()

	for _, want := range []string{
		`hpatchz drifted since the delta was built: it used "HDiffPatch::hpatchz v1.0.0", this machine has "HDiffPatch::hpatchz v9.9.9"`,
		`xz drifted since the delta was built: it used "xz (XZ Utils) 5.0.0", this machine has "xz (XZ Utils) 9.9.9"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("no warning for %q, got:\n%s", want, got)
		}
	}
	// hdiffz is recorded but no apply runs it, so drifting it says nothing
	// about this apply and must not be reported.
	if strings.Contains(got, "hdiffz drifted") {
		t.Errorf("reported hdiffz, which an apply never runs:\n%s", got)
	}
}

// TestCheckToolVersionsQuietWhenAligned is the case every ordinary apply hits:
// nothing has moved, so nothing is said.
func TestCheckToolVersionsQuietWhenAligned(t *testing.T) {
	recorded := []byte("hpatchz: HDiffPatch::hpatchz v5.1.3\nzstd: 1.5.7\n")

	var out strings.Builder
	comp := versionComp{version: "zstd: 1.5.7"}
	checkToolVersions(context.Background(), &out, recorded, comp, fakeHpatchz(t, "HDiffPatch::hpatchz v5.1.3"))
	if got := out.String(); got != "" {
		t.Errorf("warned with nothing drifted:\n%s", got)
	}
}

// TestCheckToolVersionsSurvivesMissingTool keeps the section advisory: a tool
// this machine cannot probe leaves the apply silent rather than warning about a
// version it never established.
func TestCheckToolVersionsSurvivesMissingTool(t *testing.T) {
	recorded := []byte("hpatchz: HDiffPatch::hpatchz v1.0.0\n")

	var out strings.Builder
	comp := versionComp{version: ""}
	missing := filepath.Join(t.TempDir(), "nothing-here")
	checkToolVersions(context.Background(), &out, recorded, comp, missing)
	if got := out.String(); got != "" {
		t.Errorf("warned about a tool it could not probe:\n%s", got)
	}
}
