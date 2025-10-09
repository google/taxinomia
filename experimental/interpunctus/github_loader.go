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

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type GitHubIssue struct {
	Number    int    `json:"number"`
	Title     string `json:"title"`
	State     string `json:"state"`
	CreatedAt string `json:"createdAt"`
	ClosedAt  string `json:"closedAt"`
	Comments  []struct {
		ID string `json:"id"`
	} `json:"comments"`
	Author struct {
		Login string `json:"login"`
	} `json:"author"`
	Assignees []struct {
		Login string `json:"login"`
	} `json:"assignees"`
	Labels []struct {
		Name string `json:"name"`
	} `json:"labels"`
	Body string `json:"body"`
}

func LoadGitHubIssues(filename string) (*Table, error) {
	// Read JSON file
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Parse JSON
	var issues []GitHubIssue
	if err := json.Unmarshal(data, &issues); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Create columns
	numberCol := NewColumn[uint32](&ColumnDef{
		name:        "number",
		displayName: "Issue Number",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    CompareNumbers,
		summable:    false,
	})

	stateCol := NewColumn[uint8](&ColumnDef{
		name:        "state",
		displayName: "State",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    strings.Compare,
		summable:    false,
	})

	authorCol := NewColumn[uint32](&ColumnDef{
		name:        "author",
		displayName: "Author",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    strings.Compare,
		summable:    false,
	})

	commentsCol := NewColumn[uint32](&ColumnDef{
		name:        "comments",
		displayName: "Comment Count",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    CompareNumbers,
		summable:    true,
	})

	yearCol := NewColumn[uint16](&ColumnDef{
		name:        "year",
		displayName: "Year",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    CompareNumbers,
		summable:    false,
	})

	monthCol := NewColumn[uint8](&ColumnDef{
		name:        "month",
		displayName: "Month",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    CompareNumbers,
		summable:    false,
	})

	labelCol := NewColumn[uint32](&ColumnDef{
		name:        "label",
		displayName: "Primary Label",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    strings.Compare,
		summable:    false,
	})

	hasAssigneeCol := NewColumn[uint8](&ColumnDef{
		name:        "has_assignee",
		displayName: "Has Assignee",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    strings.Compare,
		summable:    false,
	})

	labelCountCol := NewColumn[uint8](&ColumnDef{
		name:        "label_count",
		displayName: "Label Count",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    CompareNumbers,
		summable:    false,
	})

	authorTypeCol := NewColumn[uint8](&ColumnDef{
		name:        "author_type",
		displayName: "Author Type",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    strings.Compare,
		summable:    false,
	})

	// First pass: count authors to identify top contributors
	authorCounts := map[string]int{}
	for _, issue := range issues {
		author := issue.Author.Login
		if author != "" {
			authorCounts[author]++
		}
	}

	// Find top 10 authors
	topAuthors := map[string]bool{}
	type authorCount struct {
		login string
		count int
	}
	counts := []authorCount{}
	for login, count := range authorCounts {
		counts = append(counts, authorCount{login, count})
	}
	// Sort by count descending
	for i := 0; i < len(counts); i++ {
		for j := i + 1; j < len(counts); j++ {
			if counts[j].count > counts[i].count {
				counts[i], counts[j] = counts[j], counts[i]
			}
		}
	}
	for i := 0; i < 10 && i < len(counts); i++ {
		topAuthors[counts[i].login] = true
	}

	// Populate columns
	for _, issue := range issues {
		// Number
		numberCol.Append(fmt.Sprintf("%d", issue.Number))

		// State
		stateCol.Append(issue.State)

		// Author
		author := issue.Author.Login
		if author == "" {
			author = "unknown"
		}
		authorCol.Append(author)

		// Comments (count of comment array)
		commentsCol.Append(fmt.Sprintf("%d", len(issue.Comments)))

		// Parse date (ISO 8601 format: 2021-03-15T10:30:00Z)
		createdTime, err := time.Parse(time.RFC3339, issue.CreatedAt)
		if err != nil {
			yearCol.Append("0")
			monthCol.Append("0")
		} else {
			yearCol.Append(fmt.Sprintf("%d", createdTime.Year()))
			monthCol.Append(fmt.Sprintf("%d", int(createdTime.Month())))
		}

		// Primary label (first label, or "none")
		label := "none"
		if len(issue.Labels) > 0 {
			label = issue.Labels[0].Name
		}
		labelCol.Append(label)

		// Has Assignee
		if len(issue.Assignees) > 0 {
			hasAssigneeCol.Append("assigned")
		} else {
			hasAssigneeCol.Append("unassigned")
		}

		// Label Count (categorized)
		labelCount := len(issue.Labels)
		if labelCount == 0 {
			labelCountCol.Append("0")
		} else if labelCount == 1 {
			labelCountCol.Append("1")
		} else if labelCount == 2 {
			labelCountCol.Append("2")
		} else {
			labelCountCol.Append("3+")
		}

		// Author Type
		if topAuthors[author] {
			authorTypeCol.Append("top-10")
		} else {
			authorTypeCol.Append("community")
		}
	}

	// Create table
	table := &Table{
		columns: map[string]IColumn{
			"number":       numberCol,
			"state":        stateCol,
			"author":       authorCol,
			"comments":     commentsCol,
			"year":         yearCol,
			"month":        monthCol,
			"label":        labelCol,
			"has_assignee": hasAssigneeCol,
			"label_count":  labelCountCol,
			"author_type":  authorTypeCol,
		},
	}

	fmt.Printf("Loaded %d GitHub issues\n", len(issues))
	return table, nil
}
