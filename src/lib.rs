use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use log::{error, info, warn};
use regex::Regex;

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

// NEW PAGES

use lazy_static::lazy_static;
lazy_static! {
    static ref API_REGEX: Regex = Regex::new(r#","title":"(.+)","pageid":([0-9]+),"#).unwrap();
    static ref WEB_REGEX: Regex = Regex::new(r#"(?m)"wgTitle":"(.*?)",\n?"wgCurRevisionId":[0-9]+,\n?"wgRevisionId":[0-9]+,\n?"wgArticleId":([0-9]+),"#).unwrap();
}
pub const RETRY_COOLDOWN: u64 = 1;

pub async fn extract_link_info_api(url: &str) -> Page {
    let formatted_url = format_url_for_api_reqwest(url);
    let request = format!(
		"https://fr.m.wikipedia.org/w/api.php?action=query&format=json&list=search&utf8=1&formatversion=2&srnamespace=0&srlimit=1&srsearch={}", 
		formatted_url
	);

    if formatted_url.len() > 98 {
        return extract_link_info_web(url).await;
    }

    loop {
        let body = reqwest::get(&request)
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
            .replace("\n", "");

        if !body.starts_with('{') {
            std::thread::sleep(Duration::from_secs(RETRY_COOLDOWN));
            continue;
        }

        if !body.starts_with("{\"batchcomplete\":true,") || body.ends_with("\"search\":[]}}") {
            // à envoyer au web
            warn_and_log(&format!("API can't find #\"{}\"#", request));
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
                error_and_log(&format!("error: no match in body: {}\n\n\n", body));
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
    loop {
        let body = reqwest::get(&request)
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
                error_and_log(&format!(
                    "error: no match in body for url {}: \n{}\n\n\n",
                    url, body
                ));
                std::process::exit(0);
            }
        }
    }
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

// LOGGING

pub fn println_and_log(message: &str) {
    println!("{}", message);
    info!("{}", message);
}

pub fn warn_and_log(message: &str) {
    eprintln!("{}", message);
    warn!("{}", message);
}

pub fn error_and_log(message: &str) {
    eprintln!("{}", message);
    error!("{}", message);
}
