DROP TRIGGER IF EXISTS add_page_to_unexplored;
DROP TABLE IF EXISTS Unexplored;
DROP TABLE IF EXISTS Links; 
DROP INDEX IF EXISTS page_index ON Pages;
DROP TABLE IF EXISTS Pages;

CREATE TABLE IF NOT EXISTS Pages (
  id INT AUTO_INCREMENT,
  url VARCHAR(255) UNIQUE NOT NULL,
  PRIMARY KEY (id)
) AUTO_INCREMENT=0;
CREATE UNIQUE INDEX IF NOT EXISTS page_index ON Pages(id);

CREATE TABLE IF NOT EXISTS Links (
  linker INT NOT NULL,
  linked INT NOT NULL,
  PRIMARY KEY (linker, linked),
  FOREIGN KEY (linker) REFERENCES Pages(id),
  FOREIGN KEY (linked) REFERENCES Pages(id)
);

CREATE TABLE IF NOT EXISTS Unexplored (
  id INT NOT NULL,
  bugged BOOLEAN DEFAULT false,
  FOREIGN KEY (id) REFERENCES Pages(id)
);

CREATE TRIGGER add_page_to_unexplored 
AFTER INSERT ON Pages
FOR EACH ROW
INSERT INTO Unexplored VALUES (NEW.id, FALSE);

INSERT INTO Pages(url) VALUES ("France");

-- @block

SELECT * FROM Pages;

SELECT Pages.id, Pages.url FROM Unexplored
JOIN Pages ON Unexplored.id = Pages.id
WHERE Unexplored.bugged = FALSE ORDER BY Pages.id ASC;