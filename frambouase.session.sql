DROP TABLE IF EXISTS Links; 
DROP INDEX IF EXISTS page_index ON Pages;
DROP TABLE IF EXISTS Pages;

CREATE TABLE IF NOT EXISTS Pages (
  id INT,
  url CHAR(255) UNIQUE NOT NULL,
  explored BOOLEAN DEFAULT false,
  bugged BOOLEAN DEFAULT false,
  PRIMARY KEY (id)
);
CREATE UNIQUE INDEX IF NOT EXISTS page_index ON Pages(id);

CREATE TABLE IF NOT EXISTS Links (
  linker INT NOT NULL,
  linked INT NOT NULL,
  PRIMARY KEY (linker, linked),
  FOREIGN KEY (linker) REFERENCES Pages(id),
  FOREIGN KEY (linked) REFERENCES Pages(id)
);

INSERT INTO Pages (id, url) VALUES (1, "France");

-- @block
SELECT * FROM Pages;

-- @block
SELECT * FROM Links;