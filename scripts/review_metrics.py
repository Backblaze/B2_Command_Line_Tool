######################################################################
#
# File: scripts/review_metrics.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Report how much human review merged PRs needed, with reverts as the guardrail.

Run it through `nox -s review_metrics -- --days 30`. Needs `GITHUB_TOKEN` or an authenticated `gh` CLI. Counts use the
first 100 reviews and conversation comments of each PR.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import statistics
import subprocess
import sys
import urllib.request

REPO = 'Backblaze/B2_Command_Line_Tool'
QUERY = """
query($q: String!, $endCursor: String) {
  search(query: $q, type: ISSUE, first: 50, after: $endCursor) {
    pageInfo { hasNextPage endCursor }
    nodes {
      ... on PullRequest {
        number title createdAt mergedAt author { __typename login }
        reviews(first: 100) {
          nodes { author { __typename login } state submittedAt comments { totalCount } }
        }
        comments(first: 100) { nodes { author { __typename login } } }
      }
    }
  }
}
"""


def is_bot(actor: dict | None) -> bool:
    # GraphQL drops the "[bot]" suffix from Bot logins, so check the type too.
    if not actor or actor.get('__typename') == 'Bot':
        return True
    login = actor.get('login') or ''
    return login.endswith('[bot]') or 'copilot' in login.lower() or login == 'coveralls'


def is_human_reviewer(actor: dict | None, author: str | None) -> bool:
    return not is_bot(actor) and actor['login'] != author


def github_token() -> str:
    token = os.environ.get('GITHUB_TOKEN') or os.environ.get('GH_TOKEN')
    if token:
        return token
    return subprocess.check_output(['gh', 'auth', 'token'], text=True).strip()


def fetch(days: int) -> list[dict]:
    since = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
    variables = {'q': f'repo:{REPO} is:pr is:merged merged:>={since}', 'endCursor': None}
    headers = {'Authorization': f'Bearer {github_token()}', 'Content-Type': 'application/json'}
    prs = []
    while True:
        body = json.dumps({'query': QUERY, 'variables': variables}).encode()
        request = urllib.request.Request('https://api.github.com/graphql', body, headers)
        with urllib.request.urlopen(request, timeout=60) as response:
            page = json.load(response)
        if 'errors' in page:
            raise RuntimeError(page['errors'])
        search = page['data']['search']
        prs += [node for node in search['nodes'] if node]
        if not search['pageInfo']['hasNextPage']:
            return prs
        variables['endCursor'] = search['pageInfo']['endCursor']


def parse_time(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))


def summarize(values: list[float]) -> str:
    if not values:
        return 'n/a'
    values = sorted(values)
    p95 = values[min(len(values) - 1, int(round(0.95 * (len(values) - 1))))]
    return f'median {statistics.median(values):.1f}, p95 {p95:.1f}'


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--days', type=int, default=30, help='Merged in the last N days.')
    args = parser.parse_args(argv)

    prs = fetch(args.days)
    approvals, comments, waits, rounds = [], [], [], []
    reverts = 0
    for pr in prs:
        author = (pr['author'] or {}).get('login')
        reviews = pr['reviews']['nodes']
        human = [r for r in reviews if is_human_reviewer(r['author'], author)]
        approvals.append(len({r['author']['login'] for r in human if r['state'] == 'APPROVED'}))
        conversation = [
            c for c in pr['comments']['nodes'] if is_human_reviewer(c['author'], author)
        ]
        comments.append(sum(r['comments']['totalCount'] for r in human) + len(conversation))
        submitted = [parse_time(r['submittedAt']) for r in human if r['submittedAt']]
        if submitted:
            waits.append((min(submitted) - parse_time(pr['createdAt'])).total_seconds() / 3600)
        bot_reviews = [r for r in reviews if is_bot(r['author'])]
        rounds.append(len(bot_reviews))
        reverts += pr['title'].lower().startswith('revert')

    print(f'{len(prs)} PRs merged in the last {args.days} days')
    print(f'Distinct human approvers per PR: {summarize(approvals)}')
    print(f'Human comments per PR: {summarize(comments)}')
    print(f'Hours to first human review: {summarize(waits)}')
    print(f'Automated review rounds per PR: {summarize(rounds)}')
    print(f'Reverts (guardrail): {reverts}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
