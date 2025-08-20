package utils

import (
	"fmt"
	"regexp"
)

func LabelsForRqliteCluster(name string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "rqlite",
		"app.kubernetes.io/instance":   name,
		"app.kubernetes.io/component":  "database",
		"app.kubernetes.io/part-of":    "rqlite-cluster",
		"app.kubernetes.io/managed-by": "rqlite-operator",
	}
}

func ValidateQuorumRequirements(currentSize, desiredSize int32) error {
	if desiredSize == 1 {
		return nil
	}

	minimumQuorum := (currentSize / 2) + 1

	if desiredSize < minimumQuorum {
		return fmt.Errorf("scaling from %d to %d nodes would break quorum (minimum %d nodes required)",
			currentSize, desiredSize, minimumQuorum)
	}

	return nil
}

func ValidateVersionFormat(version string) error {
	if version == "" || version == "latest" {
		return nil
	}

	semverPattern := `^v?(\d+)\.(\d+)\.(\d+)(-.*)?$`
	matched, err := regexp.MatchString(semverPattern, version)
	if err != nil {
		return fmt.Errorf("version regex compilation error: %w", err)
	}

	if !matched {
		return fmt.Errorf("version must follow semantic versioning (e.g., 7.21.4 or v7.21.4)")
	}

	return nil
}

func ValidateVersionUpdate(currentVersion, targetVersion string) error {
	if currentVersion == targetVersion {
		return fmt.Errorf("target version is the same as current version: %s", currentVersion)
	}

	if err := ValidateVersionFormat(targetVersion); err != nil {
		return fmt.Errorf("invalid target version: %w", err)
	}

	return nil
}
