use chrono::{DateTime, Utc};
use octocrab::models::issues::Issue;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug)]
pub struct RepoInfo {
    pub owner: String,
    pub name: String,
}

impl RepoInfo {
    pub fn from_url(url: &str) -> Option<Self> {
        let parts: Vec<&str> = url.trim_end_matches('/').split('/').collect();
        if parts.len() >= 2 {
            Some(RepoInfo {
                owner: parts[parts.len() - 2].to_string(),
                name: parts[parts.len() - 1].to_string(),
            })
        } else {
            None
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct KudosIssue {
    pub number: i64,
    pub title: String,
    pub html_url: String,
    pub issue_created_at: DateTime<Utc>,
    pub issue_updated_at: DateTime<Utc>,
    pub user: String,
    pub labels: Vec<String>,
}

impl From<Issue> for KudosIssue {
    fn from(value: Issue) -> Self {
        KudosIssue {
            number: value.number as i64,
            title: value.title,
            html_url: value.html_url.to_string(),
            issue_created_at: value.created_at,
            issue_updated_at: value.updated_at,
            user: value.user.login,
            labels: value
                .labels
                .iter()
                .map(|label| label.name.clone())
                .collect::<Vec<String>>(),
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct Repository {
    pub label: String,
    pub url: String,
}

impl Repository {
    pub fn insert_respository_query(&self) -> &str {
        let query_string = r#"
        INSERT INTO repositories (slug, project_id)
        VALUES ($1, $2)
        RETURNING id;
        "#;
        return query_string;
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ProjectAttributes {
    pub purposes: Vec<String>,
    pub stack_levels: Vec<String>,
    pub technologies: Vec<String>,
    pub types: Vec<String>,
}

#[derive(Deserialize, Debug)]
pub struct Payload {
    pub secret: String,
    pub project_slug: String,
    pub project_name: String,
    pub repos_to_add: Vec<Repository>,
    pub repos_to_remove: Vec<Repository>,
    pub attributes: Option<ProjectAttributes>,
}
