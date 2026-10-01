/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package replication

import (
	"encoding/xml"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

// Tests replication rule addition.
func TestAddReplicationRule(t *testing.T) {
	testCases := []struct {
		cfg         Config
		opts        Options
		expectedErr string
	}{
		{ // test case :1
			cfg: Config{},
			opts: Options{
				ID:           "xyz.id",
				Prefix:       "abc/",
				RuleStatus:   "enable",
				Priority:     "3",
				TagString:    "k1=v1&k2=v2",
				StorageClass: "STANDARD",
				DestBucket:   "arn:minio:replication:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:destbucket",
			},
			expectedErr: "",
		},
		{ // test case :2
			cfg: Config{},
			opts: Options{
				ID:           "",
				Prefix:       "abc/",
				RuleStatus:   "",
				Priority:     "3",
				TagString:    "k1=v1&k2=v2",
				StorageClass: "STANDARD",
				DestBucket:   "arn:minio:replication:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:destbucket",
			},
			expectedErr: "rule state should be either [enable|disable]",
		},
		{ // test case :3
			cfg: Config{Rules: []Rule{{Priority: 1}}},
			opts: Options{
				ID:           "xyz.id",
				Prefix:       "abc/",
				RuleStatus:   "enable",
				Priority:     "1",
				TagString:    "k1=v1&k2=v2",
				StorageClass: "STANDARD",
				DestBucket:   "arn:minio:replication:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:destbucket",
			},
			expectedErr: "priority must be unique. Replication configuration already has a rule with this priority",
		},
		{ // test case :4
			cfg: Config{},
			opts: Options{
				ID:           "xyz.id",
				Prefix:       "abc/",
				RuleStatus:   "enable",
				Priority:     "3",
				TagString:    "k1=v1&k2=v2",
				StorageClass: "STANDARD",
				DestBucket:   "arn:minio:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:destbucket",
			},
			expectedErr: "destination bucket needs to be in Arn format",
		},
		{ // test case :5
			cfg: Config{},
			opts: Options{
				ID:           "xyz.id",
				Prefix:       "abc/",
				RuleStatus:   "enable",
				Priority:     "3",
				TagString:    "k1=v1&k2=v2",
				StorageClass: "STANDARD",
				DestBucket:   "arn:destbucket",
			},
			expectedErr: "destination bucket needs to be in Arn format",
		},
		{ // test case :6
			cfg: Config{Role: "arn:minio:replication:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:targetbucket"},
			opts: Options{
				ID:           "xyz.id",
				Prefix:       "abc/",
				RuleStatus:   "enable",
				Priority:     "3",
				TagString:    "k1=v1&k2=v2",
				StorageClass: "STANDARD",
				DestBucket:   "arn:minio:replication:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:destbucket",
			},
			expectedErr: "",
		},
		{ // test case :7
			cfg: Config{},
			opts: Options{
				ID:           "xyz.id",
				Prefix:       "abc/",
				RuleStatus:   "enable",
				Priority:     "3",
				TagString:    "k1=v1&k2=v2",
				StorageClass: "STANDARD",
				DestBucket:   "arn:aws:s3:::destbucket",
			},
			expectedErr: "",
		},
		{ // test case :8
			cfg: Config{
				Rules: []Rule{
					{
						ID: "xyz.id",
						Destination: Destination{
							Bucket: "arn:aws:s3:::destbucket",
						},
					},
				},
			},
			opts: Options{
				ID:           "xyz.id",
				Prefix:       "abc/",
				RuleStatus:   "enable",
				Priority:     "1",
				TagString:    "k1=v1&k2=v2",
				StorageClass: "STANDARD",
				DestBucket:   "arn:minio:replication:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:destbucket",
			},
			expectedErr: "a rule exists with this ID",
		},
	}
	for i, testCase := range testCases {
		cfg := testCase.cfg
		err := cfg.AddRule(testCase.opts)
		if err != nil && testCase.expectedErr != err.Error() {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.expectedErr, err)
		}
		if err == nil && testCase.expectedErr != "" {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.expectedErr, err)
		}
	}
}

// Tests replication rule edits.
func TestEditReplicationRule(t *testing.T) {
	testCases := []struct {
		cfg         Config
		opts        Options
		expectedErr string
	}{
		{ // test case :1 edit a rule in older config with remote ARN in destination bucket
			cfg: Config{
				Role: "arn:minio:replication:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:destbucket",
				Rules: []Rule{{
					ID:          "xyz.id",
					Priority:    1,
					Filter:      Filter{Prefix: "xyz/"},
					Destination: Destination{Bucket: "arn:aws:s3:::destbucket"},
				}},
			},
			opts: Options{
				ID:           "xyz.id",
				Prefix:       "abc/",
				RuleStatus:   "enable",
				Priority:     "3",
				TagString:    "k1=v1&k2=v2",
				StorageClass: "STANDARD",
				DestBucket:   "arn:minio:replication:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:destbucket",
			},
			expectedErr: "",
		},
		{ // test case :2 mismatched rule id
			cfg: Config{
				Role: "arn:minio:replication:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:destbucket",
				Rules: []Rule{{
					ID:          "xyz.id2",
					Priority:    1,
					Filter:      Filter{Prefix: "xyz/"},
					Destination: Destination{Bucket: "arn:aws:s3:::destbucket"},
				}},
			},
			opts: Options{
				ID:           "xyz.id",
				Prefix:       "abc/",
				RuleStatus:   "enable",
				Priority:     "3",
				TagString:    "k1=v1&k2=v2",
				StorageClass: "STANDARD",
				DestBucket:   "arn:minio:replication:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:destbucket",
			},
			expectedErr: "rule with ID xyz.id not found in replication configuration",
		},
		{ // test case :3 missing rule id
			cfg: Config{
				Role: "arn:minio:replication:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:destbucket",
				Rules: []Rule{{
					ID:          "xyz.id2",
					Priority:    1,
					Filter:      Filter{Prefix: "xyz/"},
					Destination: Destination{Bucket: "arn:minio:replication:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:destbucket"},
				}},
			},
			opts: Options{
				Prefix:       "abc/",
				RuleStatus:   "enable",
				Priority:     "3",
				TagString:    "k1=v1&k2=v2",
				StorageClass: "STANDARD",
				DestBucket:   "arn:minio:replication:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:destbucket",
			},
			expectedErr: "rule ID missing",
		},
		{ // test case :4 different destination bucket
			cfg: Config{
				Role: "",
				Rules: []Rule{{
					ID:          "xyz.id",
					Priority:    1,
					Filter:      Filter{Prefix: "xyz/"},
					Destination: Destination{Bucket: "arn:minio:replication:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:destbucket"},
				}},
			},
			opts: Options{
				ID:           "xyz.id",
				Prefix:       "abc/",
				RuleStatus:   "enable",
				Priority:     "3",
				TagString:    "k1=v1&k2=v2",
				StorageClass: "STANDARD",
				DestBucket:   "arn:aws:s3:::differentbucket",
			},
			expectedErr: "invalid destination bucket for this rule",
		},
		{ // test case :5 invalid destination bucket arn format
			cfg: Config{
				Role: "arn:minio:replication:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:destbucket",
				Rules: []Rule{{
					ID:          "xyz.id",
					Priority:    1,
					Filter:      Filter{Prefix: "xyz/"},
					Destination: Destination{Bucket: "arn:aws:s3:::destbucket"},
				}},
			},
			opts: Options{
				ID:           "xyz.id",
				Prefix:       "abc/",
				RuleStatus:   "enable",
				Priority:     "3",
				TagString:    "k1=v1&k2=v2",
				StorageClass: "STANDARD",
				DestBucket:   "arn:destbucket",
			},
			expectedErr: "destination bucket needs to be in Arn format",
		},

		{ // test case :6 invalid rule status
			cfg: Config{
				Rules: []Rule{{
					ID:          "xyz.id",
					Priority:    1,
					Filter:      Filter{Prefix: "xyz/"},
					Destination: Destination{Bucket: "arn:aws:s3:::destbucket"},
				}},
			},
			opts: Options{
				ID:           "xyz.id",
				Prefix:       "abc/",
				RuleStatus:   "xx",
				Priority:     "3",
				TagString:    "k1=v1&k2=v2",
				StorageClass: "STANDARD",
				DestBucket:   "arn:aws:s3:::destbucket",
			},
			expectedErr: "rule state should be either [enable|disable]",
		},
		{ // test case :7 another rule has same priority
			cfg: Config{
				Rules: []Rule{
					{
						ID:          "xyz.id",
						Priority:    0,
						Filter:      Filter{Prefix: "xyz/"},
						Destination: Destination{Bucket: "arn:aws:s3:::destbucket"},
					},
					{
						ID:          "xyz.id2",
						Priority:    1,
						Filter:      Filter{Prefix: "xyz/"},
						Destination: Destination{Bucket: "arn:aws:s3:::destbucket"},
					},
				},
			},
			opts: Options{
				ID:           "xyz.id",
				Prefix:       "abc/",
				RuleStatus:   "disable",
				Priority:     "1",
				TagString:    "k1=v1&k2=v2",
				StorageClass: "STANDARD",
				DestBucket:   "arn:aws:s3:::destbucket",
			},
			expectedErr: "priority must be unique. Replication configuration already has a rule with this priority",
		},
		{ // test case :8 ; edit a rule in older config
			cfg: Config{
				Role: "arn:minio:replication:eu-west-1:c5acb6ac-9918-4dc6-8534-6244ed1a611a:destbucket",
				Rules: []Rule{{
					ID:          "xyz.id",
					Priority:    1,
					Filter:      Filter{Prefix: "xyz/"},
					Destination: Destination{Bucket: "arn:aws:s3:::destbucket"},
				}},
			},
			opts: Options{
				ID:           "xyz.id",
				Prefix:       "abc/",
				RuleStatus:   "enable",
				Priority:     "3",
				TagString:    "k1=v1&k2=v2",
				StorageClass: "STANDARD",
				DestBucket:   "arn:aws:s3:::destbucket",
			},
			expectedErr: "",
		},
	}

	for i, testCase := range testCases {
		cfg := testCase.cfg
		err := cfg.EditRule(testCase.opts)
		if err != nil && testCase.expectedErr != err.Error() {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.expectedErr, err)
		}
		if err == nil && testCase.expectedErr != "" {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.expectedErr, err)
		}
	}
}

func generatedIDRuleOptions() Options {
	return Options{
		Prefix:       "abc/",
		RuleStatus:   "enable",
		Priority:     "3",
		TagString:    "k1=v1&k2=v2",
		StorageClass: "STANDARD",
		DestBucket:   "arn:aws:s3:::destbucket",
	}
}

func TestAddRuleGeneratedID(t *testing.T) {
	var ids [2]string
	for i := range ids {
		var cfg Config
		opts := generatedIDRuleOptions()
		if err := cfg.AddRule(opts); err != nil {
			t.Fatal(err)
		}
		if len(cfg.Rules) != 1 || opts.ID != "" {
			t.Fatal("expected one rule without modifying the caller's options")
		}
		ids[i] = cfg.Rules[0].ID
		parsed, err := uuid.Parse(ids[i])
		if err != nil || ids[i] == "" {
			t.Fatalf("generated ID %q is not a UUID: %v", ids[i], err)
		}
		if parsed.Version() != 4 || parsed.Variant() != uuid.RFC4122 || parsed.String() != ids[i] {
			t.Fatalf("generated ID %q is not a canonical RFC-4122 Version-4 UUID", ids[i])
		}
	}
	if ids[0] == ids[1] {
		t.Fatal("independent empty-ID calls generated the same ID")
	}
}

func TestAddRuleExplicitIDPreserved(t *testing.T) {
	var cfg Config
	opts := generatedIDRuleOptions()
	opts.ID = "Caller.ID:preserve-Case/001"
	if err := cfg.AddRule(opts); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Rules) != 1 || cfg.Rules[0].ID != opts.ID {
		t.Fatal("caller-supplied ID was changed or regenerated")
	}
}

func TestAddRuleGeneratedIDXMLRoundTrip(t *testing.T) {
	var cfg Config
	if err := cfg.AddRule(generatedIDRuleOptions()); err != nil {
		t.Fatal(err)
	}
	encoded, err := xml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Config
	if err := xml.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Rules) != 1 || decoded.Rules[0].ID == "" || decoded.Rules[0].ID != cfg.Rules[0].ID {
		t.Fatal("XML round trip changed the generated ID")
	}
	roundTrip, err := xml.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(roundTrip) != string(encoded) {
		t.Fatal("XML round trip changed the rule encoding")
	}
}

func TestGeneratedRuleEditAndPriority(t *testing.T) {
	var cfg Config
	opts := generatedIDRuleOptions()
	if err := cfg.AddRule(opts); err != nil {
		t.Fatal(err)
	}
	want := cfg.Rules[0]
	opts.ID = want.ID
	opts.Priority = "7"
	opts.RuleStatus = "disable"
	if err := cfg.EditRule(opts); err != nil {
		t.Fatal(err)
	}
	want.Priority = 7
	want.Status = Disabled
	if len(cfg.Rules) != 1 || !reflect.DeepEqual(cfg.Rules[0], want) {
		t.Fatal("edit failed to replace the matching rule while preserving its ID and other fields")
	}

	// AddRule's existing duplicate-ID and priority errors must remain exact.
	opts.Priority = "9"
	if err := cfg.AddRule(opts); err == nil || err.Error() != "a rule exists with this ID" {
		t.Fatalf("duplicate-ID behavior changed: %v", err)
	}
	opts.ID = ""
	opts.Priority = "7"
	if err := cfg.AddRule(opts); err == nil || err.Error() != "priority must be unique. Replication configuration already has a rule with this priority" {
		t.Fatalf("duplicate-priority behavior changed: %v", err)
	}
	if len(cfg.Rules) != 1 || !reflect.DeepEqual(cfg.Rules[0], want) {
		t.Fatal("rejected additions changed the existing rule")
	}
}
