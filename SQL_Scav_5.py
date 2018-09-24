import bq_helper

github = bq_helper.BigQueryHelper(active_project="bigquery-public-data", dataset_name="github_repos")

query1 = """
        WITH python_repos AS 
        (
        -- selecting distinct repo name
        SELECT DISTINCT repo_name
        FROM `bigquery-public-data.github_repos.sample_files`
        WHERE path LIKE '%.py'
        )
        SELECT COUNT(commit) as num_of_commits, sc.repo_name
        FROM `bigquery-public-data.github_repos.sample_commits` as sc
        JOIN python_repos
            ON python_repos.repo_name = sc.repo_name
        GROUP BY sc.repo_name
        ORDER BY num_of_commits DESC
        """

py_commits_df = github.query_to_pandas_safe(query1,max_gb_scanned = 10)

print(py_commits_df)
