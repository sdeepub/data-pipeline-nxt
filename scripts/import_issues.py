#!/usr/bin/env python3
"""
GitHub Issues CSV Importer
Imports issues from a CSV file into a GitHub repository.

CSV columns expected:
- title: Issue title
- body: Issue description/body
- labels: Comma-separated labels
- assignee: GitHub username
- milestone: Milestone name
- project: Project name (if any)
- column: Project column (if any)
- estimate_hours: Time estimate in hours
- story_points: Story points estimate

Usage:
    python import_issues.py <csv_file> <owner> <repo> <github_token>
    
Example:
    python import_issues.py tasks.csv sdeepub data-pipeline-nxt ghp_xxxxx
"""

import csv
import sys
import requests
from typing import Dict, List, Optional

class GitHubIssueImporter:
    def __init__(self, owner: str, repo: str, token: str):
        self.owner = owner
        self.repo = repo
        self.token = token
        self.base_url = "https://api.github.com"
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }
        self.issues_created = 0
        self.issues_failed = 0

    def create_issue(self, issue_data: Dict) -> Optional[Dict]:
        """Create a single issue in GitHub."""
        url = f"{self.base_url}/repos/{self.owner}/{self.repo}/issues"
        
        # Prepare the payload
        payload = {
            "title": issue_data.get("title", "").strip(),
            "body": self._build_body(issue_data),
        }
        
        # Add labels if present
        labels = issue_data.get("labels", "").strip()
        if labels:
            payload["labels"] = [l.strip() for l in labels.split(",")]
        
        # Add assignee if present and valid
        assignee = issue_data.get("assignee", "").strip()
        if assignee and assignee != "your-github-username":
            payload["assignee"] = assignee
        
        try:
            response = requests.post(url, json=payload, headers=self.headers, timeout=10)
            response.raise_for_status()
            created_issue = response.json()
            print(f"✓ Created issue #{created_issue['number']}: {payload['title']}")
            self.issues_created += 1
            return created_issue
        except requests.exceptions.RequestException as e:
            print(f"✗ Failed to create issue '{payload['title']}': {str(e)}")
            if hasattr(e.response, 'text'):
                print(f"  Response: {e.response.text}")
            self.issues_failed += 1
            return None

    def _build_body(self, issue_data: Dict) -> str:
        """Build the issue body with all metadata."""
        body_parts = []
        
        # Add description if present
        description = issue_data.get("body", "").strip()
        if description:
            body_parts.append(description)
            body_parts.append("")
        
        # Add metadata section
        body_parts.append("---")
        body_parts.append("### Metadata")
        
        if issue_data.get("estimate_hours", "").strip():
            body_parts.append(f"- **Estimate (hours):** {issue_data.get('estimate_hours').strip()}")
        
        if issue_data.get("story_points", "").strip():
            body_parts.append(f"- **Story Points:** {issue_data.get('story_points').strip()}")
        
        if issue_data.get("milestone", "").strip():
            body_parts.append(f"- **Milestone:** {issue_data.get('milestone').strip()}")
        
        if issue_data.get("project", "").strip():
            body_parts.append(f"- **Project:** {issue_data.get('project').strip()}")
        
        if issue_data.get("column", "").strip():
            body_parts.append(f"- **Column:** {issue_data.get('column').strip()}")
        
        return "\n".join(body_parts)

    def import_from_csv(self, csv_file: str) -> None:
        """Import issues from a CSV file."""
        print(f"Importing issues from {csv_file}...\n")
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                if not reader.fieldnames:
                    print("Error: CSV file is empty or invalid")
                    return
                
                for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is 1)
                    if not row.get("title", "").strip():
                        print(f"⚠ Skipping row {row_num}: No title found")
                        continue
                    
                    self.create_issue(row)
        
        except FileNotFoundError:
            print(f"Error: File '{csv_file}' not found")
            sys.exit(1)
        except Exception as e:
            print(f"Error reading CSV file: {str(e)}")
            sys.exit(1)
        
        print(f"\n{'='*50}")
        print(f"Import Summary")
        print(f"{'='*50}")
        print(f"✓ Created: {self.issues_created}")
        print(f"✗ Failed:  {self.issues_failed}")
        print(f"Total:   {self.issues_created + self.issues_failed}")


def main():
    if len(sys.argv) < 4:
        print("Usage: python import_issues.py <csv_file> <owner> <repo> [github_token]")
        print("\nExample:")
        print("  python import_issues.py tasks.csv sdeepub data-pipeline-nxt ghp_xxxxx")
        print("\nIf github_token is not provided, it will be read from GITHUB_TOKEN env var")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    owner = sys.argv[2]
    repo = sys.argv[3]
    
    # Get token from argument or environment
    if len(sys.argv) > 4:
        token = sys.argv[4]
    else:
        import os
        token = os.getenv("GITHUB_TOKEN")
        if not token:
            print("Error: GitHub token not provided and GITHUB_TOKEN env var not set")
            print("\nProvide token via:")
            print("  1. Command argument: python import_issues.py tasks.csv owner repo <token>")
            print("  2. Environment variable: export GITHUB_TOKEN=ghp_xxxxx")
            sys.exit(1)
    
    importer = GitHubIssueImporter(owner, repo, token)
    importer.import_from_csv(csv_file)


if __name__ == "__main__":
    main()
