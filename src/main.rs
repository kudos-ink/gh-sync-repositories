mod types;

use types::{KudosIssue, Payload, RepoInfo};

use lambda_http::{
    run, service_fn,
    tracing::{self, error},
    Body, Error, Request, Response,
};
use octocrab::{params::State, Octocrab};
use serde_json;

use sqlx::postgres::PgPool;
use sqlx::Row;
use std::env;

async fn function_handler(event: Request) -> Result<Response<Body>, Error> {
    let request_body = event.body();
    let json_string = (match request_body {
        Body::Text(json) => Some(json),
        _ => None,
    })
    .ok_or_else(|| Error::from("Invalid request body type"))?;

    let payload: Payload = serde_json::from_str(&json_string).map_err(|e| {
        error!("Error parsing JSON: {}", e);
        Error::from("Error parsing payload JSON")
    })?;

    let secret = &env::var("SECRET")?;
    if payload.secret != *secret {
        return Err(Error::from("Error: secrets don't match"));
    }

    let pool = PgPool::connect(&env::var("DATABASE_URL")?).await?;

    // get project id - need to ensure that name and slug are unique!
    let project_row = sqlx::query("SELECT id FROM projects WHERE name = $1 AND slug = $2")
        .bind(payload.project_name)
        .bind(payload.project_slug)
        .fetch_one(&pool)
        .await?;

    let project_id: i32 = project_row.get("id");

    if let Some(attributes) = payload.attributes {
        sqlx::query("UPDATE projects SET purposes = $1, stack_levels = $2, technologies = $3, types = $4 WHERE id = $5")
        .bind(attributes.purposes)
        .bind(attributes.stack_levels)
        .bind(attributes.technologies)
        .bind(attributes.types)
        .bind(project_id)
        .execute(&pool).await?;
    }

    for repo in payload.repos_to_remove {
        // This should automatically cascade to issues table
        sqlx::query("DELETE FROM repositories WHERE url = $1")
            .bind(repo.url)
            .execute(&pool)
            .await?;
    }

    if payload.repos_to_add.is_empty() {
        // return early
        return Ok(Response::builder()
            .status(200)
            .header("content-type", "text/plain")
            .body(Body::Text(format!(
                "Total issues imported: {}",
                "total_issues_imported"
            )))
            .map_err(Box::new)?);
    }

    let token = env::var("GITHUB_TOKEN")?;
    let octocrab = Octocrab::builder().personal_token(token).build()?;

    let mut total_issues_imported = 0;
    for repo in payload.repos_to_add {
        let repo_info = RepoInfo::from_url(&repo.url)
            .ok_or_else(|| Error::from("Couldn't extract repo info from url"))?;

        let repo_row = sqlx::query(repo.insert_respository_query())
            .bind(&repo.label)
            .bind(project_id)
            .fetch_one(&pool)
            .await?;
        let repo_id: i32 = repo_row.get("id");

        let page = octocrab
            .issues(repo_info.owner, repo_info.name)
            .list()
            .state(State::Open)
            .per_page(100)
            .send()
            .await?;

        let filtered_issues: Vec<KudosIssue> = page
            .items
            .into_iter()
            .filter_map(|issue| {
                issue
                    .pull_request
                    .is_none()
                    .then(|| KudosIssue::from(issue))
            })
            .collect();

        if filtered_issues.is_empty() {
            continue;
        }

        let placeholders = filtered_issues
            .iter()
            .enumerate()
            .map(|(i, _)| {
                format!(
                    "(${}, ${}, ${}, ${}, ${})",
                    i * 5 + 1,
                    i * 5 + 2,
                    i * 5 + 3,
                    i * 5 + 4,
                    i * 5 + 5
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        let query_string = format!(
            "INSERT INTO issues (number, title, labels, repository_id, issue_created_at) VALUES {}",
            placeholders
        );

        let mut insert_issues_query = sqlx::query(&query_string);

        for issue in filtered_issues {
            insert_issues_query = insert_issues_query
                .bind(issue.number)
                .bind(issue.title)
                .bind(issue.labels)
                .bind(repo_id)
                .bind(issue.issue_created_at)
        }

        let issues_inserted_count = insert_issues_query.execute(&pool).await?.rows_affected();

        total_issues_imported += issues_inserted_count;
    }

    let resp = Response::builder()
        .status(200)
        .header("content-type", "text/plain")
        .body(Body::Text(format!(
            "Total issues imported: {}",
            total_issues_imported
        )))
        .map_err(Box::new)?;
    Ok(resp)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    run(service_fn(function_handler)).await
}
