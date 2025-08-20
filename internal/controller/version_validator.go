package controller

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type VersionInfo struct {
	Major int
	Minor int
	Patch int
	Tag   string
}

const MinimumSupportedMajorVersion = 8

func ParseVersion(version string) (*VersionInfo, error) {
	if version == "" || version == "latest" {
		return &VersionInfo{Tag: version}, nil
	}

	version = strings.TrimPrefix(version, "v")

	versionRegex := regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)(?:-(.+))?$`)
	matches := versionRegex.FindStringSubmatch(version)

	if matches == nil {
		return nil, fmt.Errorf("invalid version format: %s (expected semantic version like 8.43.3)", version)
	}

	major, err := strconv.Atoi(matches[1])
	if err != nil {
		return nil, fmt.Errorf("invalid major version: %s", matches[1])
	}

	minor, err := strconv.Atoi(matches[2])
	if err != nil {
		return nil, fmt.Errorf("invalid minor version: %s", matches[2])
	}

	patch, err := strconv.Atoi(matches[3])
	if err != nil {
		return nil, fmt.Errorf("invalid patch version: %s", matches[3])
	}

	info := &VersionInfo{
		Major: major,
		Minor: minor,
		Patch: patch,
	}

	if len(matches) > 4 && matches[4] != "" {
		info.Tag = matches[4]
	}

	return info, nil
}

func ValidateVersionFormat(version string) error {
	if version == "" || version == "latest" {
		return nil
	}

	versionInfo, err := ParseVersion(version)
	if err != nil {
		return err
	}

	if versionInfo.Major < MinimumSupportedMajorVersion {
		return fmt.Errorf("rqlite version %s is not supported, minimum major version is %d",
			version, MinimumSupportedMajorVersion)
	}

	return nil
}

func ValidateVersionUpdate(currentVersion, targetVersion string) error {
	if currentVersion == targetVersion {
		return nil
	}

	if err := ValidateVersionFormat(currentVersion); err != nil {
		return fmt.Errorf("current version validation failed: %w", err)
	}

	if err := ValidateVersionFormat(targetVersion); err != nil {
		return fmt.Errorf("target version validation failed: %w", err)
	}

	if currentVersion == "latest" || targetVersion == "latest" {
		return nil
	}

	current, err := ParseVersion(currentVersion)
	if err != nil {
		return fmt.Errorf("invalid current version %s: %w", currentVersion, err)
	}

	target, err := ParseVersion(targetVersion)
	if err != nil {
		return fmt.Errorf("invalid target version %s: %w", targetVersion, err)
	}

	if current.Major > target.Major {
		return fmt.Errorf("downgrade from major version %d to %d is not supported", current.Major, target.Major)
	}

	if current.Major == target.Major && current.Major >= MinimumSupportedMajorVersion {
		return nil
	}

	if target.Major > current.Major {
		if target.Major > MinimumSupportedMajorVersion {
			return fmt.Errorf("upgrade to major version %d is not yet tested and supported", target.Major)
		}
	}

	return nil
}

func IsUpgrade(currentVersion, targetVersion string) bool {
	if currentVersion == targetVersion {
		return false
	}

	if targetVersion == "latest" {
		return true
	}
	if currentVersion == "latest" {
		return false
	}

	current, err := ParseVersion(currentVersion)
	if err != nil {
		return false
	}

	target, err := ParseVersion(targetVersion)
	if err != nil {
		return false
	}

	// Compare versions
	if target.Major > current.Major {
		return true
	}
	if target.Major < current.Major {
		return false
	}

	if target.Minor > current.Minor {
		return true
	}
	if target.Minor < current.Minor {
		return false
	}

	return target.Patch > current.Patch
}

func IsImagePullError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "imagepullbackoff") ||
		strings.Contains(errStr, "errimagepull") ||
		strings.Contains(errStr, "pull image") ||
		strings.Contains(errStr, "manifest unknown") ||
		strings.Contains(errStr, "not found") && strings.Contains(errStr, "repository")
}
