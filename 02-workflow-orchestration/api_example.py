import requests


r = requests.get("https://api.github.com/repos/kestra-io/kestra")
gh_starts = r.json()['stargazers_count']
print(gh_starts)