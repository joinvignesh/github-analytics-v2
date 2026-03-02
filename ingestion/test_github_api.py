"""
GitHub API Test Script
Tests authentication and basic API access
Run this before building the full extractor
"""

import os
import sys
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()


def test_github_token():
    """Test if GitHub token is valid and check rate limits"""

    print("=" * 60)
    print("GitHub API Authentication Test")
    print("=" * 60)

    # Get token from environment
    token = os.getenv("GITHUB_TOKEN")

    if not token:
        print("❌ ERROR: GITHUB_TOKEN not found in environment")
        print("\nTo fix:")
        print("1. Create a GitHub Personal Access Token")
        print("2. Add to .env file: GITHUB_TOKEN=ghp_your_token_here")
        return False

    print(f"✓ Token found: {token[:10]}...")

    # Test authentication
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    try:
        response = requests.get(
            "https://api.github.com/user", headers=headers, timeout=10
        )

        if response.status_code == 200:
            user_data = response.json()
            print(f"✓ Authentication successful!")
            print(f"  Authenticated as: {user_data.get('login', 'Unknown')}")
            print(f"  Account type: {user_data.get('type', 'Unknown')}")

            # Check rate limits
            rate_limit = response.headers.get("X-RateLimit-Limit")
            remaining = response.headers.get("X-RateLimit-Remaining")
            reset_timestamp = response.headers.get("X-RateLimit-Reset", "0")

            print(f"\n✓ Rate Limit Status:")
            print(f"  Limit: {rate_limit} requests/hour")
            print(f"  Remaining: {remaining}")

            # Only convert and display reset time if timestamp exists
            if reset_timestamp:
                reset_time = datetime.fromtimestamp(int(reset_timestamp))
                print(f"  Resets at: {reset_time}")

            # Check remaining with proper None handling
            if remaining and int(remaining) < 100:
                print(f"\n⚠️  WARNING: Low rate limit remaining!")

            return True

        elif response.status_code == 401:
            print(f"❌ Authentication failed: Invalid token")
            print(f"  Status: {response.status_code}")
            return False
        else:
            print(f"❌ Unexpected response: {response.status_code}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return False


def test_repository_access():
    """Test accessing a specific repository"""

    print("\n" + "=" * 60)
    print("Repository Access Test")
    print("=" * 60)

    token = os.getenv("GITHUB_TOKEN")
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    # Test repository
    test_repo = "apache/airflow"
    owner, repo = test_repo.split("/")

    print(f"\nTesting access to: {test_repo}")

    try:
        # Get repository metadata
        response = requests.get(
            f"https://api.github.com/repos/{owner}/{repo}", headers=headers, timeout=10
        )

        if response.status_code == 200:
            repo_data = response.json()
            print(f"✓ Repository found!")
            print(f"  Name: {repo_data['full_name']}")
            print(f"  Description: {repo_data['description'][:80]}...")
            print(f"  Stars: {repo_data['stargazers_count']:,}")
            print(f"  Forks: {repo_data['forks_count']:,}")
            print(f"  Open issues: {repo_data['open_issues_count']:,}")
            print(f"  Language: {repo_data.get('language', 'N/A')}")

            return True
        else:
            print(f"❌ Failed to access repository: {response.status_code}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return False


def test_issues_endpoint():
    """Test fetching issues with pagination"""

    print("\n" + "=" * 60)
    print("Issues Endpoint Test")
    print("=" * 60)

    token = os.getenv("GITHUB_TOKEN")
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    test_repo = "apache/airflow"
    owner, repo = test_repo.split("/")

    # Get issues from last 7 days
    since = (datetime.now() - timedelta(days=7)).isoformat()

    print(f"\nFetching issues since: {since}")
    print(f"Repository: {test_repo}")

    try:
        response = requests.get(
            f"https://api.github.com/repos/{owner}/{repo}/issues",
            headers=headers,
            params={
                "state": "all",
                "since": since,
                "per_page": 10,  # Small number for testing
                "page": 1,
            },
            timeout=10,
        )

        if response.status_code == 200:
            issues = response.json()
            print(f"✓ Issues fetched successfully!")
            print(f"  Count (this page): {len(issues)}")

            if issues:
                # Show first issue
                first_issue = issues[0]
                print(f"\n  Sample issue:")
                print(f"    Number: #{first_issue['number']}")
                print(f"    Title: {first_issue['title'][:60]}...")
                print(f"    State: {first_issue['state']}")
                print(f"    Author: {first_issue['user']['login']}")
                print(f"    Created: {first_issue['created_at']}")

                # Check if it's a PR
                is_pr = "pull_request" in first_issue
                print(f"    Is PR: {is_pr}")

                # Check pagination
                link_header = response.headers.get("Link", "")
                has_next = 'rel="next"' in link_header
                print(f"\n  Pagination:")
                print(f"    Has next page: {has_next}")
            else:
                print(f"  No issues found in last 7 days")

            return True
        else:
            print(f"❌ Failed to fetch issues: {response.status_code}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return False


def test_comments_endpoint():
    """Test fetching issue comments"""

    print("\n" + "=" * 60)
    print("Comments Endpoint Test")
    print("=" * 60)

    token = os.getenv("GITHUB_TOKEN")
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    test_repo = "apache/airflow"
    owner, repo = test_repo.split("/")

    since = (datetime.now() - timedelta(days=7)).isoformat()

    print(f"\nFetching comments since: {since}")

    try:
        response = requests.get(
            f"https://api.github.com/repos/{owner}/{repo}/issues/comments",
            headers=headers,
            params={"since": since, "per_page": 5, "page": 1},
            timeout=10,
        )

        if response.status_code == 200:
            comments = response.json()
            print(f"✓ Comments fetched successfully!")
            print(f"  Count (this page): {len(comments)}")

            if comments:
                first_comment = comments[0]
                print(f"\n  Sample comment:")
                print(f"    Author: {first_comment['user']['login']}")
                print(f"    Created: {first_comment['created_at']}")
                print(f"    Body preview: {first_comment['body'][:60]}...")
            else:
                print(f"  No comments found in last 7 days")

            return True
        else:
            print(f"❌ Failed to fetch comments: {response.status_code}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return False


def main():
    """Run all tests"""

    print("\n" + "🚀" * 30)
    print("GitHub API Test Suite")
    print("🚀" * 30 + "\n")

    results = []

    # Test 1: Authentication
    results.append(("Authentication", test_github_token()))

    # Test 2: Repository access
    results.append(("Repository Access", test_repository_access()))

    # Test 3: Issues endpoint
    results.append(("Issues Endpoint", test_issues_endpoint()))

    # Test 4: Comments endpoint
    results.append(("Comments Endpoint", test_comments_endpoint()))

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)

    for test_name, passed in results:
        status = "✓ PASS" if passed else "❌ FAIL"
        print(f"{status}: {test_name}")

    all_passed = all(result[1] for result in results)

    if all_passed:
        print("\n✅ All tests passed! Ready to build extraction script.")
    else:
        print("\n⚠️  Some tests failed. Fix issues before proceeding.")
        sys.exit(1)


if __name__ == "__main__":
    main()
