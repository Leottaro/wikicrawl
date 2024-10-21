use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::ops::{AddAssign, Mul};
use std::time::Duration;

use log::{error, warn};
use regex::Regex;
use reqwest::{Client, ClientBuilder};

#[derive(Debug)]
pub struct Page {
    pub id: usize,
    pub title: String,
}
impl PartialEq for Page {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Page {}

impl Hash for Page {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Display for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ \"id\": {}, \"title\": \"{}\" }}",
            self.id,
            self.title.replace("\"", "\\\"")
        )
    }
}

impl Clone for Page {
    fn clone(&self) -> Self {
        Page {
            id: self.id,
            title: self.title.clone(),
        }
    }
}

// NEW PAGES

pub const RETRY_COOLDOWN: Duration = Duration::from_secs(3);

use lazy_static::lazy_static;
lazy_static! {
    static ref API_REGEX: Regex = Regex::new(r#","title":"(.+)","pageid":([0-9]+),"#).unwrap();
    static ref WEB_REGEX: Regex = Regex::new(
        r#"(?m)"wgTitle":\n?"(.*?)",\n?"wgCurRevisionId":\n?[0-9]+,\n?"wgRevisionId":\n?([0-9]+),"#
    )
    .unwrap();
    pub static ref EXPLORE_REGEX: Regex =
        Regex::new(r#"(?m)"\/wiki\/([^"\/]+?)(?:#.+?)?"[ >]"#).unwrap();
    pub static ref CLIENT: Client = ClientBuilder::new()
        .connect_timeout(RETRY_COOLDOWN.mul(10))
        .connection_verbose(true)
        .build()
        .unwrap();
}

pub async fn extract_link_info_api(url: &str) -> Page {
    let formatted_url = format_url_for_api_reqwest(url);
    let request = format!(
		"https://fr.m.wikipedia.org/w/api.php?action=query&format=json&list=search&utf8=1&formatversion=2&srnamespace=0&srlimit=1&srsearch={}", 
		formatted_url
	);

    if formatted_url.len() > 98 {
        return extract_link_info_web(url).await;
    }

    let mut retry_cooldown = RETRY_COOLDOWN.clone();
    let delta_t = Duration::from_secs(1);
    loop {
        retry_cooldown.add_assign(delta_t);
        let body = CLIENT
            .get(&request)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
            .replace("\n", "");

        if !body.starts_with('{') {
            warn!("link info api of url {} throwed wikimedia error", url);
            std::thread::sleep(retry_cooldown);
            continue;
        }

        if !body.starts_with("{\"batchcomplete\":true,") || body.ends_with("\"search\":[]}}") {
            // à envoyer au web
            warn!("API can't find #\"{}\"# with body\n{}", request, body);
            return extract_link_info_web(url).await;
        }

        let captures = API_REGEX.captures(&body);
        match captures {
            Some(capture) => {
                return Page {
                    title: capture.get(1).unwrap().as_str().replace("\\\"", "\""),
                    id: capture.get(2).unwrap().as_str().parse::<usize>().unwrap(),
                };
            }
            None => {
                error!("error: no match in body: {}\n\n\n", body);
                std::process::exit(0);
            }
        }
    }
}

async fn extract_link_info_web(url: &str) -> Page {
    let request = format!(
        "https://fr.m.wikipedia.org/wiki/Spécial:Recherche/{}",
        format_url_for_reqwest(url)
    );

    let body = CLIENT
        .get(&request)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
        .replace("\n", "");

    let captures = WEB_REGEX.captures(&body);
    match captures {
        Some(capture) => {
            return Page {
                title: capture.get(1).unwrap().as_str().to_string(),
                id: capture.get(2).unwrap().as_str().parse::<usize>().unwrap(),
            };
        }
        None => {
            error!("no match in body for url {}: \n{}\n\n\n", url, body);
            std::process::exit(0);
        }
    }
}

pub fn format_link_for_mysql(link: &String) -> String {
    link.chars()
        .map(|char| match char {
            '\\' => "\\\\".to_string(),
            '"' => "\"\"".to_string(),
            _ => char.to_string(),
        })
        .collect()
}

fn format_url_for_api_reqwest(url: &str) -> String {
    url.chars()
        .map(|char| match char {
            '&' => "%26".to_string(),
            '*' => "\\*".to_string(),
            _ => char.to_string(),
        })
        .collect()
}

fn format_url_for_reqwest(url: &str) -> String {
    url.chars()
        .map(|char| match char {
            '?' => "%3F".to_string(),
            _ => char.to_string(),
        })
        .collect()
}
