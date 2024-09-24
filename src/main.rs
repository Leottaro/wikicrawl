pub mod wikicrawl;
use wikicrawl::setup_wikicrawl;

use mysql::Pool;
use std::{
    collections::HashMap,
    env,
    io::{stdin, stdout, Error, ErrorKind, Write},
    usize,
};

const ENV_PATH: &str = ".env";
const ENV_DEFAULT: &str =
    "WIKICRAWL_USER=root\nWIKICRAWL_PASSWORD=root\nWIKICRAWL_HOST=localhost\nWIKICRAWL_PORT=3306\nWIKICRAWL_EXPLORING_PAGES=10\nWIKICRAWL_NEW_PAGES=80\n";

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_BACKTRACE", "1");
    let args: Vec<String> = env::args().collect();
    let mut command_line_argument = args
        .get(1)
        .unwrap_or(&"0".to_string())
        .parse::<usize>()
        .unwrap_or(0);

    let (database_url, max_exploring_pages, max_new_pages) = get_env().unwrap();

    println!("connecting to database");
    let pool = Pool::new(database_url.clone().as_str()).unwrap();
    let mut connection = pool.get_conn().unwrap();

    let mut user_input = String::new();
    loop {
        if command_line_argument == 0 {
            print!("\nWhat do you want to do ?\n1: Search the smallest path between two pages\n2: Crawl wikipedia\n3: Exit\nYou Choose: ");
            stdout().flush().unwrap();
            user_input.clear();
            stdin()
                .read_line(&mut user_input)
                .expect("Failed to read your choice");
        } else {
            user_input = command_line_argument.to_string();
            command_line_argument = 0;
        }
        match user_input.trim().parse::<usize>() {
            Ok(1) => println!("Wikipath is not implemented yet"),
            Ok(2) => setup_wikicrawl(&mut connection, max_exploring_pages, max_new_pages).await,
            Ok(3) => println!("Exiting the program"),
            _ => {
                println!("Please enter a valid number.");
                continue;
            }
        }
        return;
    }
}

fn get_env() -> Result<(String, usize, usize), Error> {
    let env_read = std::fs::read_to_string(ENV_PATH);
    if env_read.is_err() {
        let env_write = std::fs::write(ENV_PATH, ENV_DEFAULT);
        if env_write.is_err() {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("Couldn't create .env file: {}", env_write.unwrap_err()),
            ));
        }
        return Err(Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "Error: .env file not found, created a new one with default values:\n{}",
                ENV_DEFAULT
            ),
        ));
    }

    let env_content = env_read.unwrap();
    if env_content.is_empty() {
        return Err(Error::new(
				ErrorKind::InvalidData,
				format!("Error: .env file is empty, delete it to see default values or fill it with the following values:\n{}", ENV_DEFAULT),
			));
    }

    let vars = env_content
        .split("\n")
        .filter_map(|line| line.split_once("="))
        .map(|(str1, str2)| (str1.to_string().split_off(10), str2.to_string()))
        .collect::<HashMap<String, String>>();

    if !vars.contains_key("USER")
        || !vars.contains_key("PASSWORD")
        || !vars.contains_key("HOST")
        || !vars.contains_key("PORT")
        || !vars.contains_key("EXPLORING_PAGES")
        || !vars.contains_key("NEW_PAGES")
    {
        return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("Error: .env file is missing some values, delete it to see default values or fill it with the following values:\n{}", ENV_DEFAULT),
			));
    }

    let connection_url = if vars["PASSWORD"].is_empty() {
        format!(
            "mysql://{}@{}:{}/wikicrawl",
            vars["USER"], vars["HOST"], vars["PORT"]
        )
    } else {
        format!(
            "mysql://{}:{}@{}:{}/wikicrawl",
            vars["USER"], vars["PASSWORD"], vars["HOST"], vars["PORT"]
        )
    };

    Ok((
        connection_url,
        vars["EXPLORING_PAGES"].parse::<usize>().unwrap_or(75),
        vars["NEW_PAGES"].parse::<usize>().unwrap_or(80),
    ))
}
